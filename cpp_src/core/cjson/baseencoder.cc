#include "baseencoder.h"
#include <cstdlib>
#include <limits>
#include "cjsonbuilder.h"
#include "cjsontools.h"
#include "core/keyvalue/p_string.h"
#include "jsonbuilder.h"
#include "msgpackbuilder.h"
#include "protobufbuilder.h"
#include "tagsmatcher.h"
#include "tools/serializer.h"

namespace reindexer {

template <typename Builder>
BaseEncoder<Builder>::BaseEncoder(const TagsMatcher* tagsMatcher, const FieldsSet* filter) : tagsMatcher_(tagsMatcher), filter_(filter) {
	static_assert(std::numeric_limits<decltype(objectScalarIndexes_)>::digits >= maxIndexes,
				  "objectScalarIndexes_ needs to provide 'maxIndexes' bits or more");
}

template <typename Builder>
void BaseEncoder<Builder>::Encode(std::string_view tuple, Builder& builder, IAdditionalDatasource<Builder>* ds) {
	Serializer rdser(tuple);
	builder.SetTagsMatcher(tagsMatcher_);
	builder.SetTagsPath(&curTagsPath_);

	ctag begTag = rdser.GetVarUint();
	(void)begTag;
	assertrx(begTag.Type() == TAG_OBJECT);
	Builder objNode = builder.Object(nullptr);
	while (encode(nullptr, rdser, objNode, true))
		;
	if (ds) {
		assertrx(!ds->GetJoinsDatasource());
		ds->PutAdditionalFields(objNode);
	}
}

template <typename Builder>
void BaseEncoder<Builder>::Encode(ConstPayload* pl, Builder& builder, IAdditionalDatasource<Builder>* ds) {
	Serializer rdser(getPlTuple(pl));
	if (rdser.Eof()) {
		return;
	}

	objectScalarIndexes_ = 0;
	for (int i = 0; i < pl->NumFields(); ++i) fieldsoutcnt_[i] = 0;
	builder.SetTagsMatcher(tagsMatcher_);
	builder.SetTagsPath(&curTagsPath_);
	ctag begTag = rdser.GetVarUint();
	(void)begTag;
	assertrx(begTag.Type() == TAG_OBJECT);
	Builder objNode = builder.Object(nullptr);
	while (encode(pl, rdser, objNode, true))
		;

	if (ds) {
		if (const auto joinsDs = ds->GetJoinsDatasource()) {
			for (size_t i = 0; i < joinsDs->GetJoinedRowsCount(); ++i) {
				encodeJoinedItems(objNode, joinsDs, i);
			}
		}
		ds->PutAdditionalFields(objNode);
	}
}

template <typename Builder>
const TagsLengths& BaseEncoder<Builder>::GetTagsMeasures(ConstPayload* pl, IEncoderDatasourceWithJoins* ds) {
	tagsLengths_.clear();
	Serializer rdser(getPlTuple(pl));
	if (!rdser.Eof()) {
		ctag beginTag = rdser.GetVarUint();
		(void)beginTag;
		assertrx(beginTag.Type() == TAG_OBJECT);

		tagsLengths_.reserve(maxIndexes);
		tagsLengths_.push_back(StartObject);

		while (collectTagsSizes(pl, rdser)) {
		}

		if (ds && ds->GetJoinedRowsCount() > 0) {
			for (size_t i = 0; i < ds->GetJoinedRowsCount(); ++i) {
				collectJoinedItemsTagsSizes(ds, i);
			}
		}

		size_t endPos = 0;
		computeObjectLength(tagsLengths_, 0, endPos);
	}
	return tagsLengths_;
}

template <typename Builder>
void BaseEncoder<Builder>::collectJoinedItemsTagsSizes(IEncoderDatasourceWithJoins* ds, size_t rowid) {
	const size_t itemsCount = ds->GetJoinedRowItemsCount(rowid);
	if (!itemsCount) return;

	string nsTagName("joined_" + ds->GetJoinedItemNamespace(rowid));
	BaseEncoder<Builder> subEnc(&ds->GetJoinedItemTagsMatcher(rowid), &ds->GetJoinedItemFieldsFilter(rowid));
	for (size_t i = 0; i < itemsCount; ++i) {
		ConstPayload pl(ds->GetJoinedItemPayload(rowid, i));
		subEnc.GetTagsMeasures(&pl, nullptr);
	}
}

template <typename Builder>
void BaseEncoder<Builder>::encodeJoinedItems(Builder& builder, IEncoderDatasourceWithJoins* ds, size_t rowid) {
	const size_t itemsCount = ds->GetJoinedRowItemsCount(rowid);
	if (!itemsCount) return;

	string nsTagName("joined_" + ds->GetJoinedItemNamespace(rowid));
	auto arrNode = builder.Array(nsTagName);

	BaseEncoder<Builder> subEnc(&ds->GetJoinedItemTagsMatcher(rowid), &ds->GetJoinedItemFieldsFilter(rowid));
	for (size_t i = 0; i < itemsCount; ++i) {
		ConstPayload pl(ds->GetJoinedItemPayload(rowid, i));
		subEnc.Encode(&pl, arrNode);
	}
}

template <typename Builder>
bool BaseEncoder<Builder>::encode(ConstPayload* pl, Serializer& rdser, Builder& builder, bool visible) {
	const ctag tag = rdser.GetVarUint();
	const int tagType = tag.Type();

	if (tagType == TAG_END) {
		return false;
	}
	const int tagName = tag.Name();

	TagsPathScope<TagsPath> pathScope(curTagsPath_, tagName);
	TagsPathScope<IndexedTagsPath> indexedPathScope(indexedTagsPath_, tagName);
	if (tagName && filter_) {
		visible = visible && filter_->match(indexedTagsPath_);
	}

	const int tagField = tag.Field();

	// get field from indexed field
	if (tagField >= 0) {
		if ((objectScalarIndexes_ & (1ULL << tagField)) && (tagType != TAG_ARRAY)) {
			string fieldName;
			if (tag.Name() && tagsMatcher_) {
				fieldName = tagsMatcher_->tag2name(tag.Name());
			}
			throw Error(errParams, "Non-array field '%s' [%d] can only be encoded once.", fieldName, tagField);
		}
		objectScalarIndexes_ |= (1ULL << tagField);
		assertrx(tagField < pl->NumFields());
		int* cnt = &fieldsoutcnt_[tagField];
		switch (tagType) {
			case TAG_ARRAY: {
				int count = rdser.GetVarUint();
				if (visible) {
					switch (pl->Type().Field(tagField).Type()) {
						case KeyValueBool:
							builder.Array(tagName, pl->GetArray<bool>(tagField).subspan((*cnt), count), *cnt);
							break;
						case KeyValueInt:
							builder.Array(tagName, pl->GetArray<int>(tagField).subspan((*cnt), count), *cnt);
							break;
						case KeyValueInt64:
							builder.Array(tagName, pl->GetArray<int64_t>(tagField).subspan((*cnt), count), *cnt);
							break;
						case KeyValueDouble:
							builder.Array(tagName, pl->GetArray<double>(tagField).subspan((*cnt), count), *cnt);
							break;
						case KeyValueString:
							builder.Array(tagName, pl->GetArray<p_string>(tagField).subspan((*cnt), count), *cnt);
							break;
						default:
							std::abort();
					}
				}
				(*cnt) += count;
				break;
			}
			case TAG_NULL:
				if (visible) builder.Null(tagName);
				break;
			default:
				if (visible) builder.Put(tagName, pl->Get(tagField, (*cnt)));
				(*cnt)++;
				break;
		}
	} else {
		switch (tagType) {
			case TAG_ARRAY: {
				carraytag atag = rdser.GetUInt32();
				if (atag.Tag() == TAG_OBJECT) {
					auto arrNode = visible ? builder.Array(tagName) : Builder();
					for (int i = 0; i < atag.Count(); i++) {
						indexedTagsPath_.back().SetIndex(i);
						encode(pl, rdser, arrNode, visible);
					}
				} else if (visible) {
					builder.Array(tagName, rdser, atag.Tag(), atag.Count());
				} else {
					for (int i = 0; i < atag.Count(); i++) rdser.GetRawVariant(KeyValueType(atag.Tag()));
				}
				break;
			}
			case TAG_OBJECT: {
				objectScalarIndexes_ = 0;
				auto objNode = visible ? builder.Object(tagName) : Builder();
				while (encode(pl, rdser, objNode, visible))
					;
				break;
			}
			default: {
				Variant value = rdser.GetRawVariant(KeyValueType(tagType));
				if (visible) builder.Put(tagName, value);
			}
		}
	}

	return true;
}

template <typename Builder>
bool BaseEncoder<Builder>::collectTagsSizes(ConstPayload* pl, Serializer& rdser) {
	const ctag tag = rdser.GetVarUint();
	const int tagType = tag.Type();
	if (tagType == TAG_END) {
		tagsLengths_.push_back(EndObject);
		return false;
	}
	const int tagName = tag.Name();

	if (tagName && filter_) {
		curTagsPath_.push_back(tagName);
	}

	int tagField = tag.Field();
	tagsLengths_.push_back(kStandardFieldSize);

	// get field from indexed field
	if (tagField >= 0) {
		assertrx(tagField < pl->NumFields());
		switch (tagType) {
			case TAG_ARRAY: {
				int count = rdser.GetVarUint();
				tagsLengths_.back() = count;
				break;
			}
			default:
				break;
		}
	} else {
		switch (tagType) {
			case TAG_ARRAY: {
				carraytag atag = rdser.GetUInt32();
				tagsLengths_.back() = atag.Count();
				if (atag.Tag() == TAG_OBJECT) {
					for (int i = 0; i < atag.Count(); i++) {
						tagsLengths_.push_back(StartArrayItem);
						collectTagsSizes(pl, rdser);
						tagsLengths_.push_back(EndArrayItem);
					}
				} else {
					for (int i = 0; i < atag.Count(); i++) {
						rdser.GetRawVariant(KeyValueType(atag.Tag()));
					}
				}
				break;
			}
			case TAG_OBJECT: {
				tagsLengths_.back() = StartObject;
				while (collectTagsSizes(pl, rdser)) {
				}
				break;
			}
			default: {
				rdser.GetRawVariant(KeyValueType(tagType));
			}
		}
	}
	if (tagName && filter_) curTagsPath_.pop_back();

	return true;
}

template <typename Builder>
std::string_view BaseEncoder<Builder>::getPlTuple(ConstPayload* pl) {
	VariantArray kref;
	pl->Get(0, kref);

	p_string tuple(kref[0]);

	if (tagsMatcher_ && tuple.size() == 0) {
		tmpPlTuple_.Reset();
		buildPayloadTuple(pl, tagsMatcher_, tmpPlTuple_);
		return tmpPlTuple_.Slice();
	}

	return std::string_view(tuple);
}

template class BaseEncoder<JsonBuilder>;
template class BaseEncoder<CJsonBuilder>;
template class BaseEncoder<MsgPackBuilder>;
template class BaseEncoder<ProtobufBuilder>;
template class BaseEncoder<FieldsExtractor>;

}  // namespace reindexer
