#pragma once

#include <optional>
#include "core/type_consts.h"
#include "estl/fast_hash_map.h"
#include "estl/span.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

class JsonBuilder;

/// Object of this class contains filters set. Filters are separated by namespace and concatenated with disjunction
class UpdatesFilters {
public:
	class Filter {
	public:
		// TODO: Any additional condition check should be added here
		bool Check() const { return true; }
		void FromJSON(const gason::JsonNode &) {}
		void GetJSON(JsonBuilder &) const {}

		bool operator==(const Filter &) const { return true; }
	};

	/// Merge two filters sets
	/// If one of the filters set is empty, result filters set will also be empty
	/// If one of the filters set contains some conditions for specific namespace,
	/// then result filters set will also contain this conditions
	/// @param rhs - Another filters set
	void Merge(const UpdatesFilters &rhs);
	/// Add new filter for specified namespace. Doesn't merge filters, just concatenates it into disjunction sequence
	/// @param ns - Namespace
	/// @param filter - Filter to add
	void AddFilter(std::string_view ns, Filter filter);
	/// Check if filters set allows this namespace
	/// @param ns - Namespace
	/// @return 'true' if filter's conditions are satisfied
	bool Check(std::string_view ns) const;

	Error FromJSON(span<char> json);
	void FromJSON(const gason::JsonNode &root);
	void GetJSON(WrSerializer &ser) const;

	bool operator==(const UpdatesFilters &rhs) const;

private:
	using FiltersList = h_vector<Filter, 4>;

	fast_hash_map<std::string, FiltersList, nocase_hash_str, nocase_equal_str, nocase_less_str> filters_;
};

class EventSubscriberConfig {
public:
	struct StreamConfig {
		UpdatesFilters filters;
		bool withConfigNamespace = false;
	};
	using StreamsContainerT = std::vector<std::optional<StreamConfig>>;

	Error FromJSON(span<char> json);
	void FromJSON(const gason::JsonNode &root);
	void GetJSON(WrSerializer &ser) const;

	const StreamsContainerT &Streams() const noexcept { return streams_; }
	bool WithDBName() const noexcept { return withDBName_; }

private:
	int formatVersion_ = kSubscribersConfigFormatVersion;
	bool withDBName_ = false;
	StreamsContainerT streams_;
};

}  // namespace reindexer
