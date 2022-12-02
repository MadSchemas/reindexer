#include "payloadfieldtype.h"
#include "core/keyvalue/p_string.h"
#include "estl/one_of.h"
#include "payloadfieldvalue.h"

namespace reindexer {

size_t PayloadFieldType::Sizeof() const noexcept {
	if (IsArray()) return sizeof(PayloadFieldValue::Array);
	return ElemSizeof();
}

size_t PayloadFieldType::ElemSizeof() const noexcept {
	return Type().EvaluateOneOf(
		[](KeyValueType::Bool) noexcept { return sizeof(bool); }, [](KeyValueType::Int) noexcept { return sizeof(int); },
		[](KeyValueType::Int64) noexcept { return sizeof(int64_t); }, [](KeyValueType::Double) noexcept { return sizeof(double); },
		[](KeyValueType::String) noexcept { return sizeof(p_string); },
		[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>) noexcept -> size_t {
			assertrx(0);
			abort();
		});
}

size_t PayloadFieldType::Alignof() const noexcept {
	if (IsArray()) return alignof(PayloadFieldValue::Array);
	return Type().EvaluateOneOf(
		[](KeyValueType::Bool) noexcept { return alignof(bool); }, [](KeyValueType::Int) noexcept { return alignof(int); },
		[](KeyValueType::Int64) noexcept { return alignof(int64_t); }, [](KeyValueType::Double) noexcept { return alignof(double); },
		[](KeyValueType::String) noexcept { return alignof(p_string); },
		[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>) noexcept -> size_t {
			assertrx(0);
			abort();
		});
}

}  // namespace reindexer
