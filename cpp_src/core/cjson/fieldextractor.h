#pragma once

#include "core/payload/fieldsset.h"
#include "tagsmatcher.h"

namespace reindexer {

class FieldsExtractor {
public:
	FieldsExtractor() = default;
	FieldsExtractor(VariantArray *va, KeyValueType expectedType, int expectedPathDepth, FieldsSet *filter = nullptr, int *index = nullptr,
					int *size = nullptr)
		: values_(va),
		  expectedType_(expectedType),
		  expectedPathDepth_(expectedPathDepth),
		  tagsPath_(nullptr),
		  filter_(filter),
		  index_(index),
		  length_(size) {}
	FieldsExtractor(FieldsExtractor &&other) = default;
	FieldsExtractor(const FieldsExtractor &) = delete;
	FieldsExtractor &operator=(const FieldsExtractor &) = delete;
	FieldsExtractor &operator=(FieldsExtractor &&) = delete;

	void SetTagsMatcher(const TagsMatcher *) {}

	FieldsExtractor Object(int) { return FieldsExtractor(values_, expectedType_, expectedPathDepth_ - 1, filter_, index_, length_); }
	FieldsExtractor Array(int) { return FieldsExtractor(values_, expectedType_, expectedPathDepth_ - 1, filter_, index_, length_); }
	FieldsExtractor Object(std::string_view) {
		return FieldsExtractor(values_, expectedType_, expectedPathDepth_ - 1, filter_, index_, length_);
	}
	FieldsExtractor Object(std::nullptr_t) { return Object(std::string_view{}); }
	FieldsExtractor Array(std::string_view) {
		return FieldsExtractor(values_, expectedType_, expectedPathDepth_ - 1, filter_, index_, length_);
	}

	template <typename T>
	void Array(int, span<T> data, int offset) {
		const IndexedPathNode &pathNode = getArrayPathNode();
		if (index_ && length_) {
			*index_ = offset;
			*length_ = data.size();
			if (pathNode.IsWithIndex()) {
				*index_ += pathNode.Index();
			}
		}
		int i = 0;
		for (auto d : data) {
			if (pathNode.IsForAllItems() || i == pathNode.Index()) {
				Put(0, Variant(d));
			}
			++i;
		}
	}

	void Array(int, Serializer &ser, int tagType, int count) {
		const IndexedPathNode &pathNode = getArrayPathNode();
		for (int i = 0; i < count; ++i) {
			Variant value = ser.GetRawVariant(KeyValueType::FromNumber(tagType));
			if (pathNode.IsForAllItems() || i == pathNode.Index()) {
				Put(0, std::move(value));
			}
		}
	}

	FieldsExtractor &Put(int, Variant arg) {
		if (expectedPathDepth_ > 0) return *this;
		expectedType_.EvaluateOneOf(
			[&](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::String,
					  KeyValueType::Null, KeyValueType::Tuple>) { arg.convert(expectedType_); },
			[](OneOf<KeyValueType::Undefined, KeyValueType::Composite>) noexcept {});
		values_->push_back(std::move(arg));
		if (expectedPathDepth_ < 0) values_->MarkObject();
		return *this;
	}

	FieldsExtractor &Null(int) { return *this; }

protected:
	const IndexedPathNode &getArrayPathNode() const {
		if (filter_ && filter_->getTagsPathsLength() > 0) {
			size_t lastItemIndex = filter_->getTagsPathsLength() - 1;
			if (filter_->isTagsPathIndexed(lastItemIndex)) {
				const IndexedTagsPath &path = filter_->getIndexedTagsPath(lastItemIndex);
				assertrx(path.size() > 0);
				if (path.back().IsArrayNode()) return path.back();
			}
		}
		static const IndexedPathNode commonNode{IndexedPathNode::AllItems};
		return commonNode;
	}

	VariantArray *values_ = nullptr;
	KeyValueType expectedType_;
	int expectedPathDepth_ = 0;
	IndexedTagsPath *tagsPath_;
	FieldsSet *filter_;
	int *index_;
	int *length_;
};

}  // namespace reindexer
