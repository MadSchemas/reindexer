#pragma once

#include "aggregationresult.h"
#include "core/item.h"
#include "core/namespace/stringsholder.h"
#include "core/payload/payloadvalue.h"
#include "core/rdxcontext.h"
#include "itemref.h"
#include "tools/serializer.h"

namespace reindexer {

class Schema;
class TagsMatcher;
class PayloadType;
class WrSerializer;
class NsContext;
struct ResultFetchOpts;
struct ItemImplRawData;
class SelectFunctionsHolder;
class NamespaceImpl;
struct CsvOrdering;

namespace joins {
class NamespaceResults;
class ItemIterator;
}  // namespace joins

/// LocalQueryResults is an interface for iterating over documents, returned by Query from Reindexer.<br>
/// *Lifetime*: LocalQueryResults uses Copy-On-Write semantics, so it has independent lifetime and state - e.g., acquired from Reindexer.
/// LocalQueryResults cannot be externaly changed or deleted even in case of changing origin data in DB.<br>
/// *Thread safety*: LocalQueryResults is thread safe.

class LocalQueryResults {
public:
	using NamespaceImplPtr = intrusive_ptr<NamespaceImpl>;

	LocalQueryResults();
	LocalQueryResults(const ItemRefVector::const_iterator &b, const ItemRefVector::const_iterator &e);
	LocalQueryResults(std::initializer_list<ItemRef> l);
	LocalQueryResults(const LocalQueryResults &) = delete;
	LocalQueryResults(LocalQueryResults &&) noexcept;
	~LocalQueryResults();
	LocalQueryResults &operator=(const LocalQueryResults &) = delete;
	LocalQueryResults &operator=(LocalQueryResults &&obj) noexcept;
	void Add(const ItemRef &);
	// use enableHold = false only if you are sure that the LocalQueryResults will be destroyed before the item
	// or if data from the item are contained in namespace added to the LocalQueryResults
	// enableHold is ignored when withData = false
	void AddItem(Item &item, bool withData = false, bool enableHold = true);
	std::string Dump() const;
	void Erase(ItemRefVector::iterator begin, ItemRefVector::iterator end);
	size_t Count() const noexcept { return items_.size(); }
	size_t TotalCount() const noexcept { return totalCount; }
	const std::string &GetExplainResults() const { return explainResults; }
	const std::vector<AggregationResult> &GetAggregationResults() const { return aggregationResults; }
	void Clear();
	h_vector<std::string_view, 1> GetNamespaces() const;
	bool IsCacheEnabled() const { return !nonCacheableData; }
	void SetOutputShardId(int shardId) noexcept { outputShardId = shardId; }
	CsvOrdering MakeCSVTagOrdering(unsigned limit, unsigned offset) const;

	class Iterator {
	public:
		Error GetJSON(WrSerializer &wrser, bool withHdrLen = true);
		Error GetCJSON(WrSerializer &wrser, bool withHdrLen = true);
		Error GetMsgPack(WrSerializer &wrser, bool withHdrLen = true);
		Error GetProtobuf(WrSerializer &wrser, bool withHdrLen = true);
		[[nodiscard]] Error GetCSV(WrSerializer &wrser, CsvOrdering &ordering) noexcept;

		// use enableHold = false only if you are sure that the item will be destroyed before the LocalQueryResults
		Item GetItem(bool enableHold = true);
		joins::ItemIterator GetJoined();
		const ItemRef &GetItemRef() const noexcept { return qr_->items_[idx_]; }
		lsn_t GetLSN() const noexcept { return qr_->items_[idx_].Value().GetLSN(); }
		bool IsRaw() const noexcept;
		std::string_view GetRaw() const noexcept;
		Iterator &operator++() noexcept;
		Iterator &operator+(int delta) noexcept;
		const Error &Status() const noexcept { return err_; }
		bool operator!=(const Iterator &) const noexcept;
		bool operator==(const Iterator &) const noexcept;
		Iterator &operator*() noexcept { return *this; }

		const LocalQueryResults *qr_;
		int idx_;
		Error err_;
	};

	Iterator begin() const { return Iterator{this, 0, errOK}; }
	Iterator end() const { return Iterator{this, int(items_.size()), errOK}; }
	Iterator operator[](int idx) const { return Iterator{this, idx, errOK}; }

	std::vector<joins::NamespaceResults> joined_;
	std::vector<AggregationResult> aggregationResults;
	int totalCount = 0;
	bool haveRank = false;
	bool nonCacheableData = false;
	bool needOutputRank = false;
	int outputShardId = ShardingKeyType::ProxyOff;	// flag+value

	struct Context;
	// precalc context size
	static constexpr int kSizeofContext = 264;	// sizeof(PayloadType) + sizeof(TagsMatcher) + sizeof(FieldsSet) + sizeof(shared_ptr);

	// Order of storing contexts for namespaces:
	// [0]      - main NS context
	// [1;N]    - contexts of all the merged namespaces
	// [N+1; M] - contexts of all the joined namespaces for all the merged namespaces:
	using ContextsVector = h_vector<Context, 1, kSizeofContext>;
	ContextsVector ctxs;

	void addNSContext(const PayloadType &type, const TagsMatcher &tagsMatcher, const FieldsSet &fieldsFilter,
					  std::shared_ptr<const Schema> schema);
	const TagsMatcher &getTagsMatcher(int nsid) const noexcept;
	const PayloadType &getPayloadType(int nsid) const noexcept;
	const FieldsSet &getFieldsFilter(int nsid) const noexcept;
	TagsMatcher &getTagsMatcher(int nsid) noexcept;
	PayloadType &getPayloadType(int nsid) noexcept;
	std::shared_ptr<const Schema> getSchema(int nsid) const noexcept;
	int getNsNumber(int nsid) const noexcept;
	int getMergedNSCount() const noexcept;
	ItemRefVector &Items() noexcept { return items_; }
	const ItemRefVector &Items() const noexcept { return items_; }
	int GetJoinedNsCtxIndex(int nsid) const noexcept;

	void SaveRawData(ItemImplRawData &&);

	// Add owning ns pointer
	// noLock has always to be 'true' (i.e. this method can only be called unders Namespace's lock)
	void AddNamespace(NamespaceImplPtr, bool noLock);
	// Add non-owning ns pointer
	// noLock has always to be 'true' (i.e. this method can only be called unders Namespace's lock)
	void AddNamespace(NamespaceImpl *, bool noLock);
	void RemoveNamespace(const NamespaceImpl *ns);
	bool IsNamespaceAdded(const NamespaceImpl *ns) const noexcept {
		return std::find_if(nsData_.cbegin(), nsData_.cend(), [ns](const NsDataHolder &nsData) { return nsData.ns == ns; }) !=
			   nsData_.cend();
	}

	std::string explainResults;

protected:
	class EncoderDatasourceWithJoins;
	class EncoderAdditionalDatasource;

private:
	void encodeJSON(int idx, WrSerializer &ser) const;
	ItemRefVector items_;
	std::vector<ItemImplRawData> rawDataHolder_;
	friend SelectFunctionsHolder;
	class NsDataHolder {
	public:
		NsDataHolder(NamespaceImplPtr &&_ns, StringsHolderPtr &&strHldr) noexcept;
		NsDataHolder(NamespaceImpl *_ns, StringsHolderPtr &&strHldr) noexcept;
		NsDataHolder(const NsDataHolder &) = delete;
		NsDataHolder(NsDataHolder &&) noexcept = default;
		NsDataHolder &operator=(const NsDataHolder &) = delete;
		NsDataHolder &operator=(NsDataHolder &&) = default;

	private:
		NamespaceImplPtr nsPtr_;

	public:
		NamespaceImpl *ns;
		StringsHolderPtr strHolder;
	};

	h_vector<NsDataHolder, 1> nsData_;
	std::vector<key_string> stringsHolder_;
};

}  // namespace reindexer
