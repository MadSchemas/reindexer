#include "namespacedef.h"
#include <unordered_map>
#include "cjson/jsonbuilder.h"
#include "namespace/namespace.h"
#include "string.h"
#include "tools/errors.h"
#include "tools/jsontools.h"

namespace reindexer {

using namespace std::string_view_literals;

Error NamespaceDef::FromJSON(span<char> json) {
	try {
		FromJSON(gason::JsonParser().Parse(json));
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "NamespaceDef: %s", ex.what());
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

void NamespaceDef::FromJSON(const gason::JsonNode &root) {
	name = root["name"].As<std::string>();
	storage.Enabled(root["storage"]["enabled"].As<bool>(true));
	storage.DropOnFileFormatError(root["storage"]["drop_on_file_format_error"].As<bool>());
	storage.CreateIfMissing(root["storage"]["create_if_missing"].As<bool>(true));

	for (auto &arrelem : root["indexes"]) {
		IndexDef idx;
		idx.FromJSON(arrelem);
		indexes.push_back(idx);
	}
	isTemporary = root["temporary"].As<bool>(false);
	schemaJson = root["schema"].As<std::string>(schemaJson);
}

void NamespaceDef::GetJSON(WrSerializer &ser, int formatFlags) const {
	JsonBuilder json(ser);
	json.Put("name", name);
	json.Object("storage").Put("enabled", storage.IsEnabled());
	{
		auto arr = json.Array("indexes");
		for (auto &idx : indexes) {
			arr.Raw(nullptr, "");
			idx.GetJSON(ser, formatFlags);
		}
	}
	json.Put("temporary", isTemporary);
	if (!schemaJson.empty()) {
		json.Put("schema", schemaJson);
	}
}

bool EnumNamespacesOpts::MatchFilter(std::string_view nsName, const std::shared_ptr<Namespace> &ns, const RdxContext &ctx) const {
	return MatchNameFilter(nsName) && (!IsHideTemporary() || !ns->IsTemporary(ctx));
}

Error NsReplicationOpts::FromJSON(span<char> json) {
	try {
		FromJSON(gason::JsonParser().Parse(json));
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "NamespaceDef: %s", ex.what());
	} catch (const Error &err) {
		return err;
	}
	return Error();
}

void NsReplicationOpts::FromJSON(const gason::JsonNode &root) {
	auto &tmNode = root["state_token"sv];
	if (!tmNode.empty()) {
		tmStateToken.emplace(tmNode.As<int32_t>());
	}
	nsVersion = lsn_t(root["ns_version"sv].As<int64_t>());
}

void NsReplicationOpts::GetJSON(WrSerializer &ser) const {
	JsonBuilder json(ser);
	if (tmStateToken.has_value()) {
		json.Put("state_token"sv, tmStateToken.value());
	}
	json.Put("ns_version"sv, int64_t(nsVersion));
}

}  // namespace reindexer
