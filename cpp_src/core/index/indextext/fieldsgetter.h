#pragma once
#include "core/index/payload_map.h"
#include "core/payload/fieldsset.h"
#include "vendor/utf8cpp/utf8.h"

namespace reindexer {

class FieldsGetter {
public:
	FieldsGetter(const FieldsSet &fields, const PayloadType &plt, KeyValueType type) : fields_(fields), plt_(plt), type_(type) {}


	RVector<std::pair<std::string_view, uint32_t>, 8> getDocFields(const key_string &doc, std::vector<std::unique_ptr<std::string>> &) {
		if (!utf8::is_valid(doc->cbegin(), doc->cend())) throw Error(errParams, "Invalid UTF8 string in FullText index");

		return {{std::string_view(*doc.get()), 0}};
	}

	VariantArray krefs;

	// Specific implemetation for composite index

	RVector<std::pair<std::string_view, uint32_t>, 8> getDocFields(const PayloadValue &doc, std::vector<std::unique_ptr<std::string>> &strsBuf) {
		ConstPayload pl(plt_, doc);

		uint32_t fieldPos = 0;
		size_t tagsPathIdx = 0;

		RVector<std::pair<std::string_view, uint32_t>, 8> ret;

		for (auto field : fields_) {
			krefs.resize(0);
			bool fieldFromCjson = (field == IndexValueType::SetByJsonPath);
			if (fieldFromCjson) {
				assertrx(tagsPathIdx < fields_.getTagsPathsLength());
				pl.GetByJsonPath(fields_.getTagsPath(tagsPathIdx++), krefs, type_);
			} else {
				pl.Get(field, krefs);
			}
			for (const Variant &kref : krefs) {
				if (!kref.Type().Is<KeyValueType::String>()) {
					strsBuf.emplace_back(std::unique_ptr<std::string>(new std::string(kref.As<std::string>())));
					ret.emplace_back(*strsBuf.back().get(), fieldPos);
				} else {
					const std::string_view stringRef(kref);
					if (!utf8::is_valid(stringRef.data(), stringRef.data() + stringRef.size()))
						throw Error(errParams, "Invalid UTF8 string in FullTextindex");
					ret.emplace_back(stringRef, fieldPos);
				}
			}
			++fieldPos;
		}
		return ret;
	}

private:
	const FieldsSet &fields_;
	const PayloadType &plt_;

	KeyValueType type_;
};
}  // namespace reindexer
