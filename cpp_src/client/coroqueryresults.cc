#include "client/coroqueryresults.h"
#include "client/namespace.h"
#include "core/cjson/baseencoder.h"
#include "core/keyvalue/p_string.h"
#include "net/cproto/coroclientconnection.h"

namespace reindexer {
namespace client {

using namespace reindexer::net;

CoroQueryResults::CoroQueryResults(int fetchFlags) noexcept
	: conn_(nullptr), queryID_(0), fetchOffset_(0), fetchFlags_(fetchFlags), fetchAmount_(0), requestTimeout_(0) {}

CoroQueryResults::CoroQueryResults(net::cproto::CoroClientConnection *conn, NsArray &&nsArray, int fetchFlags, int fetchAmount,
								   milliseconds timeout)
	: conn_(conn),
	  nsArray_(std::move(nsArray)),
	  queryID_(0),
	  fetchOffset_(0),
	  fetchFlags_(fetchFlags),
	  fetchAmount_(fetchAmount),
	  requestTimeout_(timeout) {}

CoroQueryResults::CoroQueryResults(net::cproto::CoroClientConnection *conn, NsArray &&nsArray, std::string_view rawResult, int queryID,
								   int fetchFlags, int fetchAmount, milliseconds timeout)
	: CoroQueryResults(conn, std::move(nsArray), fetchFlags, fetchAmount, timeout) {
	Bind(rawResult, queryID);
}

CoroQueryResults::CoroQueryResults(NsArray &&nsArray, Item &item) : CoroQueryResults() {
	nsArray_ = std::move(nsArray);
	queryParams_.totalcount = 0;
	queryParams_.qcount = 1;
	queryParams_.count = 1;
	queryParams_.flags = kResultsCJson;
	std::string_view itemData = item.GetCJSON();
	rawResult_.resize(itemData.size() + sizeof(uint32_t));
	uint32_t dataSize = itemData.size();
	memcpy(&rawResult_[0], &dataSize, sizeof(uint32_t));
	memcpy(&rawResult_[0] + sizeof(uint32_t), itemData.data(), itemData.size());
}

void CoroQueryResults::Bind(std::string_view rawResult, int queryID) {
	queryID_ = queryID;
	ResultSerializer ser(rawResult);

	try {
		ser.GetRawQueryParams(queryParams_, [&ser, this](int nsIdx) {
			const uint32_t stateToken = ser.GetVarUint();
			const int version = ser.GetVarUint();
			TagsMatcher newTm;
			newTm.deserialize(ser, version, stateToken);
			nsArray_[nsIdx]->TryReplaceTagsMatcher(std::move(newTm));
			// nsArray[nsIdx]->payloadType_.clone()->deserialize(ser);
			// nsArray[nsIdx]->tagsMatcher_.updatePayloadType(nsArray[nsIdx]->payloadType_, false);
			PayloadType("tmp").clone()->deserialize(ser);
		});
	} catch (const Error &err) {
		status_ = err;
	}

	rawResult_.assign(rawResult.begin() + ser.Pos(), rawResult.end());
}

void CoroQueryResults::fetchNextResults() {
	using std::chrono::seconds;
	int flags = fetchFlags_ ? (fetchFlags_ & ~kResultsWithPayloadTypes) : kResultsCJson;
	auto ret =
		conn_->Call({cproto::kCmdFetchResults, requestTimeout_, milliseconds(0), lsn_t(), -1, IndexValueType::NotSet, nullptr, false},
					queryID_, flags, queryParams_.count + fetchOffset_, fetchAmount_);
	if (!ret.Status().ok()) {
		throw ret.Status();
	}

	auto args = ret.GetArgs(2);

	fetchOffset_ += queryParams_.count;

	std::string_view rawResult = p_string(args[0]);
	ResultSerializer ser(rawResult);

	ser.GetRawQueryParams(queryParams_, nullptr);

	rawResult_.assign(rawResult.begin() + ser.Pos(), rawResult.end());
}

h_vector<std::string_view, 1> CoroQueryResults::GetNamespaces() const {
	h_vector<std::string_view, 1> ret;
	ret.reserve(nsArray_.size());
	for (auto &ns : nsArray_) ret.emplace_back(ns->name);
	return ret;
}

TagsMatcher CoroQueryResults::GetTagsMatcher(int nsid) const noexcept {
	assert(nsid < int(nsArray_.size()));
	return nsArray_[nsid]->GetTagsMatcher();
}

TagsMatcher CoroQueryResults::GetTagsMatcher(std::string_view nsName) const noexcept {
	auto it = std::find_if(nsArray_.begin(), nsArray_.end(), [&nsName](Namespace *ns) { return (std::string_view(ns->name) == nsName); });
	if (it == nsArray_.end()) {
		return TagsMatcher();
	}
	return (*it)->GetTagsMatcher();
}

PayloadType CoroQueryResults::GetPayloadType(int nsid) const noexcept {
	assert(nsid < int(nsArray_.size()));
	return nsArray_[nsid]->payloadType;
}

PayloadType CoroQueryResults::GetPayloadType(std::string_view nsName) const noexcept {
	auto it = std::find_if(nsArray_.begin(), nsArray_.end(), [&nsName](Namespace *ns) { return (std::string_view(ns->name) == nsName); });
	if (it == nsArray_.end()) {
		return PayloadType();
	}
	return (*it)->payloadType;
}

class AdditionalRank : public IAdditionalDatasource<JsonBuilder> {
public:
	AdditionalRank(double r) : rank_(r) {}
	void PutAdditionalFields(JsonBuilder &builder) const final { builder.Put("rank()", rank_); }
	IEncoderDatasourceWithJoins *GetJoinsDatasource() final { return nullptr; }

private:
	double rank_;
};

void CoroQueryResults::Iterator::getJSONFromCJSON(std::string_view cjson, WrSerializer &wrser, bool withHdrLen) const {
	auto tm = qr_->GetTagsMatcher(itemParams_.nsid);
	JsonEncoder enc(&tm);
	JsonBuilder builder(wrser, ObjType::TypePlain);
	if (qr_->NeedOutputRank()) {
		AdditionalRank additionalRank(itemParams_.proc);
		if (withHdrLen) {
			auto slicePosSaver = wrser.StartSlice();
			enc.Encode(cjson, builder, &additionalRank);
		} else {
			enc.Encode(cjson, builder, &additionalRank);
		}
	} else {
		if (withHdrLen) {
			auto slicePosSaver = wrser.StartSlice();
			enc.Encode(cjson, builder, nullptr);
		} else {
			enc.Encode(cjson, builder, nullptr);
		}
	}
}

Error CoroQueryResults::Iterator::GetMsgPack(WrSerializer &wrser, bool withHdrLen) {
	readNext();
	int type = qr_->queryParams_.flags & kResultsFormatMask;
	if (type == kResultsMsgPack) {
		if (withHdrLen) {
			wrser.PutSlice(itemParams_.data);
		} else {
			wrser.Write(itemParams_.data);
		}
	} else {
		return Error(errParseBin, "Impossible to get data in MsgPack because of a different format: %d", type);
	}
	return errOK;
}

Error CoroQueryResults::Iterator::GetJSON(WrSerializer &wrser, bool withHdrLen) {
	readNext();
	try {
		switch (qr_->queryParams_.flags & kResultsFormatMask) {
			case kResultsCJson: {
				getJSONFromCJSON(itemParams_.data, wrser, withHdrLen);
				break;
			}
			case kResultsJson:
				if (withHdrLen) {
					wrser.PutSlice(itemParams_.data);
				} else {
					wrser.Write(itemParams_.data);
				}
				break;
			default:
				return Error(errParseBin, "Server returned data in unknown format %d", qr_->queryParams_.flags & kResultsFormatMask);
		}
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Error CoroQueryResults::Iterator::GetCJSON(WrSerializer &wrser, bool withHdrLen) {
	readNext();
	try {
		switch (qr_->queryParams_.flags & kResultsFormatMask) {
			case kResultsCJson:
				if (withHdrLen) {
					wrser.PutSlice(itemParams_.data);
				} else {
					wrser.Write(itemParams_.data);
				}
				break;
			case kResultsMsgPack:
				return Error(errParseBin, "Server returned data in msgpack format, can't process");
			case kResultsJson:
				return Error(errParseBin, "Server returned data in json format, can't process");
			default:
				return Error(errParseBin, "Server returned data in unknown format %d", qr_->queryParams_.flags & kResultsFormatMask);
		}
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Item CoroQueryResults::Iterator::GetItem() {
	readNext();
	Error err;
	try {
		Item item = qr_->nsArray_[itemParams_.nsid]->NewItem();
		item.setID(itemParams_.id);
		switch (qr_->queryParams_.flags & kResultsFormatMask) {
			case kResultsMsgPack: {
				size_t offset = 0;
				err = item.FromMsgPack(itemParams_.data, offset);
				break;
			}
			case kResultsCJson: {
				err = item.FromCJSON(itemParams_.data);
				break;
			}
			case kResultsJson: {
				char *endp = nullptr;
				err = item.FromJSON(itemParams_.data, &endp);
				break;
			}
			case kResultsPure:
				break;
			default:
				return Item();
		}
		if (err.ok()) {
			return item;
		}
	} catch (const Error &err) {
		return Item(err);
	}
	return Item(std::move(err));
}

int64_t CoroQueryResults::Iterator::GetLSN() {
	readNext();
	return itemParams_.lsn;
}

bool CoroQueryResults::Iterator::IsRaw() {
	readNext();
	return itemParams_.raw;
}

std::string_view CoroQueryResults::Iterator::GetRaw() {
	readNext();
	assert(itemParams_.raw);
	return itemParams_.data;
}

void CoroQueryResults::Iterator::readNext() {
	if (nextPos_ != 0) return;

	std::string_view rawResult(qr_->rawResult_.data(), qr_->rawResult_.size());
	ResultSerializer ser(rawResult.substr(pos_));

	try {
		itemParams_ = ser.GetItemData(qr_->queryParams_.flags);
		joinedData_.clear();
		if (qr_->queryParams_.flags & kResultsWithJoined) {
			int format = qr_->queryParams_.flags & kResultsFormatMask;
			(void)format;
			assert(format == kResultsCJson);
			int joinedFields = ser.GetVarUint();
			for (int i = 0; i < joinedFields; ++i) {
				int itemsCount = ser.GetVarUint();
				h_vector<ResultSerializer::ItemParams, 1> joined;
				joined.reserve(itemsCount);
				for (int j = 0; j < itemsCount; ++j) {
					joined.emplace_back(ser.GetItemData(qr_->queryParams_.flags));
				}
				joinedData_.emplace_back(std::move(joined));
			}
		}
		nextPos_ = pos_ + ser.Pos();
	} catch (const Error &err) {
		const_cast<CoroQueryResults *>(qr_)->status_ = err;
	}
}

CoroQueryResults::Iterator &CoroQueryResults::Iterator::operator++() {
	try {
		readNext();
		idx_++;
		pos_ = nextPos_;
		nextPos_ = 0;
		if (idx_ != qr_->queryParams_.qcount && idx_ == qr_->queryParams_.count + qr_->fetchOffset_) {
			const_cast<CoroQueryResults *>(qr_)->fetchNextResults();
			pos_ = 0;
		}
	} catch (const Error &err) {
		const_cast<CoroQueryResults *>(qr_)->status_ = err;
	}

	return *this;
}

}  // namespace client
}  // namespace reindexer
