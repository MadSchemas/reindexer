#include "event_subscriber_config.h"
#include "core/cjson/jsonbuilder.h"

namespace reindexer {

using namespace std::string_view_literals;

Error EventSubscriberConfig::FromJSON(span<char> json) {
	try {
		FromJSON(gason::JsonParser().Parse(json));
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "UpdatesFilter: %s", ex.what());
	} catch (const Error &err) {
		return err;
	}
	return {};
}

void EventSubscriberConfig::FromJSON(const gason::JsonNode &root) {
	formatVersion_ = root["version"sv].As<int>(-1);
	if (formatVersion_ < kMinSubscribersConfigFormatVersion) {
		throw Error(errParams, "Min supported subscribers config format version is %d, but %d version was found in JSON",
					kMinSubscribersConfigFormatVersion, formatVersion_)
	}
	streams_.clear();
	streams_.resize(kMaxStreamsPerSub);

	withDBName_ = root["with_db_name"sv].As<bool>(false);
	for (const auto &stream : root["streams"sv]) {
		const int id = stream["id"].As<int>(-1);
		if (id < 0 || id >= streams_.size()) {
			throw Error(errParams, "Stream ID %d is out of range", id);
		}
		if (streams_[id].has_value()) {
			throw Error(errParams, "Stream ID %d is duplicated", id);
		}

		auto &s = streams_[id].emplace();
		s.withConfigNamespace = stream["with_config_namespace"sv].As<bool>(false);
		for (const auto &ns : stream["namespaces"sv]) {
			auto name = ns["name"sv].As<std::string_view>();
			for (const auto &f : ns["filters"sv]) {
				UpdatesFilters::Filter filter;
				filter.FromJSON(f);
				s.filters.AddFilter(name, std::move(filter));
			}
		}
	}
}

void EventSubscriberConfig::GetJSON(WrSerializer &ser) const {
	JsonBuilder builder(ser);
	{
		builder.Put("version"sv, formatVersion_);
		builder.Put("with_config_namespace"sv, withDBName_);
		auto streamArr = builder.Array("streams"sv);
		for (size_t i = 0; i < streams_.size(); ++i) {
			auto streamObj = streamArr.Object();
			if (streams_[i].has_value()) {
				streamObj.Put("with_config_namespace"sv, streams_[i]->withConfigNamespace);
				auto nsArr = streamObj.Array("namespaces"sv);
				for (const auto &nsFilters : streams_[i]->filters) {
					auto obj = nsArr.Object();
					obj.Put("name"sv, nsFilters.first);
					auto arrFilters = obj.Array("filters"sv);
					for (const auto &filter : nsFilters.second) {
						auto filtersObj = arrFilters.Object();
						filter.GetJSON(filtersObj);
					}
				}
			}
		}
	}
}

}  // namespace reindexer
