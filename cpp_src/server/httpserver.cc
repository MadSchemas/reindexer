#include "httpserver.h"
#include <sys/stat.h>
#include <iomanip>
#include <sstream>
#include "base64/base64.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/msgpackdecoder.h"
#include "core/cjson/protobufbuilder.h"
#include "core/cjson/protobufschemabuilder.h"
#include "core/itemimpl.h"
#include "core/namespace/namespace.h"
#include "core/queryresults/tableviewbuilder.h"
#include "core/schema.h"
#include "core/type_consts.h"
#include "gason/gason.h"
#include "loggerwrapper.h"
#include "net/http/serverconnection.h"
#include "net/listener.h"
#include "reindexer_version.h"
#include "resources_wrapper.h"
#include "statscollect/istatswatcher.h"
#include "statscollect/prometheus.h"
#include "tools/alloc_ext/je_malloc_extension.h"
#include "tools/alloc_ext/tc_malloc_extension.h"
#include "tools/fsops.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "wal/walrecord.h"

#include "outputparameters.h"

using std::string;
using std::stringstream;
using namespace std::string_view_literals;

namespace reindexer_server {

constexpr size_t kTxIdLen = 20;
constexpr auto kTxDeadlineCheckPeriod = std::chrono::seconds(1);

HTTPServer::HTTPServer(DBManager &dbMgr, LoggerWrapper &logger, const ServerConfig &serverConfig, Prometheus *prometheus,
					   IStatsWatcher *statsWatcher)
	: dbMgr_(dbMgr),
	  serverConfig_(serverConfig),
	  prometheus_(prometheus),
	  statsWatcher_(statsWatcher),
	  webRoot_(reindexer::fs::JoinPath(serverConfig.WebRoot, "")),
	  logger_(logger),
	  startTs_(std::chrono::system_clock::now()) {}

Error HTTPServer::execSqlQueryByType(std::string_view sqlQuery, bool &isWALQuery, reindexer::QueryResults &res, http::Context &ctx) {
	reindexer::Query q;
	q.FromSQL(sqlQuery);
	isWALQuery = q.IsWALQuery();
	std::string_view sharding = ctx.request->params.Get("sharding");
	switch (q.Type()) {
		case QuerySelect: {
			auto db =
				sharding == "off"sv
					? getDB(ctx, kRoleDataRead).WithTimeout(serverConfig_.HttpReadTimeout).WithShardId(ShardingKeyType::ProxyOff, false)
					: getDB(ctx, kRoleDataRead).WithTimeout(serverConfig_.HttpReadTimeout);
			return db.Select(q, res);
		}
		case QueryDelete: {
			auto db =
				sharding == "off"sv
					? getDB(ctx, kRoleDataWrite).WithTimeout(serverConfig_.HttpWriteTimeout()).WithShardId(ShardingKeyType::ProxyOff, false)
					: getDB(ctx, kRoleDataWrite).WithTimeout(serverConfig_.HttpWriteTimeout());

			return db.Delete(q, res);
		}
		case QueryUpdate: {
			auto db =
				sharding == "off"sv
					? getDB(ctx, kRoleDataWrite).WithTimeout(serverConfig_.HttpWriteTimeout()).WithShardId(ShardingKeyType::ProxyOff, false)
					: getDB(ctx, kRoleDataWrite).WithTimeout(serverConfig_.HttpWriteTimeout());
			return db.Update(q, res);
		}
		case QueryTruncate: {
			auto db =
				sharding == "off"sv
					? getDB(ctx, kRoleDBAdmin).WithTimeout(serverConfig_.HttpWriteTimeout()).WithShardId(ShardingKeyType::ProxyOff, false)
					: getDB(ctx, kRoleDBAdmin).WithTimeout(serverConfig_.HttpWriteTimeout());

			return db.TruncateNamespace(q._namespace);
		}
		default:
			throw Error(errParams, "unknown query type %d", q.Type());
	}
}

int HTTPServer::GetSQLQuery(http::Context &ctx) {
	string sqlQuery = urldecode2(ctx.request->params.Get("q"));

	std::string_view limitParam = ctx.request->params.Get("limit");
	std::string_view offsetParam = ctx.request->params.Get("offset");

	unsigned limit = prepareLimit(limitParam);
	unsigned offset = prepareOffset(offsetParam);

	if (sqlQuery.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Missed `q` parameter"));
	}
	reindexer::QueryResults res;

	bool isWALQuery = false;
	auto ret = execSqlQueryByType(sqlQuery, isWALQuery, res, ctx);
	if (!ret.ok()) {
		return status(ctx, http::HttpStatus(ret));
	}
	return queryResults(ctx, res, isWALQuery, true, limit, offset);
}

int HTTPServer::GetSQLSuggest(http::Context &ctx) {
	string sqlQuery = urldecode2(ctx.request->params.Get("q"));
	if (sqlQuery.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Missed `q` parameter"));
	}

	std::string_view posParam = ctx.request->params.Get("pos");
	std::string_view lineParam = ctx.request->params.Get("line");
	int pos = stoi(posParam);
	if (pos < 0) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "`pos` parameter should be >= 0"));
	}
	int line = stoi(lineParam);
	if (line < 0) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "`line` parameter should be >= 0"));
	}

	size_t bytePos = 0;
	Error err = cursosPosToBytePos(sqlQuery, line, pos, bytePos);
	if (!err.ok()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, err.what()));
	}

	logPrintf(LogTrace, "GetSQLSuggest() incoming data: %s, %d", sqlQuery, bytePos);

	vector<string> suggestions;
	auto db = getDB(ctx, kRoleDataRead).WithTimeout(serverConfig_.HttpReadTimeout);
	db.GetSqlSuggestions(sqlQuery, bytePos, suggestions);

	WrSerializer ser(ctx.writer->GetChunk());
	reindexer::JsonBuilder builder(ser);
	auto node = builder.Array("suggests");
	for (auto &suggest : suggestions) node.Put(nullptr, suggest);
	node.End();
	builder.End();

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::PostSQLQuery(http::Context &ctx) {
	reindexer::QueryResults res;
	string sqlQuery = ctx.body->Read();
	if (!sqlQuery.length()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Query is empty"));
	}
	bool isWALQuery = false;
	auto ret = execSqlQueryByType(sqlQuery, isWALQuery, res, ctx);
	if (!ret.ok()) {
		return status(ctx, http::HttpStatus(http::StatusInternalServerError, ret.what()));
	}
	return queryResults(ctx, res, isWALQuery, true);
}

int HTTPServer::PostQuery(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDataRead).WithTimeout(serverConfig_.HttpReadTimeout);
	reindexer::QueryResults res;
	string dsl = ctx.body->Read();

	reindexer::Query q;
	auto err = q.FromJSON(dsl);
	if (!err.ok()) {
		return jsonStatus(ctx, http::HttpStatus(err));
	}

	err = db.Select(q, res);
	if (!err.ok()) {
		return jsonStatus(ctx, http::HttpStatus(err));
	}
	return queryResults(ctx, res, q.IsWALQuery(), true);
}

int HTTPServer::DeleteQuery(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDataWrite).WithTimeout(serverConfig_.HttpWriteTimeout());
	string dsl = ctx.body->Read();

	reindexer::Query q;
	auto status = q.FromJSON(dsl);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	reindexer::QueryResults res;
	status = db.Delete(q, res);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}
	WrSerializer ser(ctx.writer->GetChunk());
	reindexer::JsonBuilder builder(ser);
	builder.Put("updated", res.Count());
	builder.End();

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::UpdateQuery(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDataWrite).WithTimeout(serverConfig_.HttpWriteTimeout());
	string dsl = ctx.body->Read();

	reindexer::Query q;
	auto status = q.FromJSON(dsl);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	reindexer::QueryResults res;
	status = db.Update(q, res);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}
	WrSerializer ser(ctx.writer->GetChunk());
	reindexer::JsonBuilder builder(ser);
	builder.Put("updated", res.Count());
	builder.End();

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::GetDatabases(http::Context &ctx) {
	std::string_view sortOrder = ctx.request->params.Get("sort_order");

	auto dbs = dbMgr_.EnumDatabases();

	int sortDirection = 0;
	if (sortOrder == "asc") {
		sortDirection = 1;
	} else if (sortOrder == "desc") {
		sortDirection = -1;
	} else if (sortOrder.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Invalid `sort_order` parameter"));
	}

	if (sortDirection) {
		std::sort(dbs.begin(), dbs.end(), [sortDirection](const string &lhs, const string &rhs) {
			if (sortDirection > 0)
				return collateCompare(lhs, rhs, CollateOpts(CollateASCII)) < 0;
			else
				return collateCompare(lhs, rhs, CollateOpts(CollateASCII)) > 0;
		});
	}

	WrSerializer ser(ctx.writer->GetChunk());
	{
		JsonBuilder builder(ser);
		builder.Put("total_items", dbs.size());
		auto arrNode = builder.Array("items");
		for (auto &db : dbs) arrNode.Put(nullptr, db);
	}

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::PostDatabase(http::Context &ctx) {
	string newDbName = getNameFromJson(ctx.body->Read());

	auto dbs = dbMgr_.EnumDatabases();
	for (auto &db : dbs) {
		if (db == newDbName) {
			return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Database already exists"));
		}
	}

	AuthContext dummyCtx;
	AuthContext *actx = &dummyCtx;
	if (!dbMgr_.IsNoSecurity()) {
		auto clientData = dynamic_cast<HTTPClientData *>(ctx.clientData.get());
		assert(clientData);
		actx = &clientData->auth;
	}

	auto status = dbMgr_.OpenDatabase(newDbName, *actx, true);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	return jsonStatus(ctx);
}

int HTTPServer::DeleteDatabase(http::Context &ctx) {
	string dbName(urldecode2(ctx.request->urlParams[0]));

	AuthContext dummyCtx;
	AuthContext *actx = &dummyCtx;
	if (!dbMgr_.IsNoSecurity()) {
		auto clientData = dynamic_cast<HTTPClientData *>(ctx.clientData.get());
		assert(clientData);
		actx = &clientData->auth;
	}

	auto status = dbMgr_.Login(dbName, *actx);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusUnauthorized, status.what()));
	}

	status = dbMgr_.DropDatabase(*actx);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	return jsonStatus(ctx);
}

int HTTPServer::GetNamespaces(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDataRead).WithTimeout(serverConfig_.HttpReadTimeout);

	std::string_view sortOrder = ctx.request->params.Get("sort_order");

	vector<reindexer::NamespaceDef> nsDefs;
	db.EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames());

	int sortDirection = 0;
	if (sortOrder == "asc") {
		sortDirection = 1;
	} else if (sortOrder == "desc") {
		sortDirection = -1;
	} else if (sortOrder.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Invalid `sort_order` parameter"));
	}

	if (sortDirection) {
		std::sort(nsDefs.begin(), nsDefs.end(), [sortDirection](const NamespaceDef &lhs, const NamespaceDef &rhs) {
			if (sortDirection > 0)
				return collateCompare(lhs.name, rhs.name, CollateOpts(CollateASCII)) < 0;
			else
				return collateCompare(lhs.name, rhs.name, CollateOpts(CollateASCII)) > 0;
		});
	}

	WrSerializer ser(ctx.writer->GetChunk());
	{
		JsonBuilder builder(ser);
		builder.Put("total_items", nsDefs.size());
		auto arrNode = builder.Array("items");
		for (auto &nsDef : nsDefs) {
			auto objNode = arrNode.Object(nullptr);
			objNode.Put("name", nsDef.name);
		}
	}
	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::GetNamespace(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDataRead).WithTimeout(serverConfig_.HttpReadTimeout);

	string nsName = urldecode2(ctx.request->urlParams[1]);

	if (!nsName.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	vector<reindexer::NamespaceDef> nsDefs;
	db.EnumNamespaces(nsDefs, EnumNamespacesOpts().WithFilter(nsName));

	if (nsDefs.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusNotFound, "Namespace is not found"));
	}

	WrSerializer wrSer(ctx.writer->GetChunk());
	nsDefs[0].GetJSON(wrSer);
	return ctx.JSON(http::StatusOK, wrSer.DetachChunk());
}

int HTTPServer::PostNamespace(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDBAdmin).WithTimeout(serverConfig_.HttpWriteTimeout());
	reindexer::NamespaceDef nsdef("");

	std::string body = ctx.body->Read();
	auto status = nsdef.FromJSON(giftStr(body));
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	status = db.AddNamespace(nsdef);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	return jsonStatus(ctx);
}

int HTTPServer::DeleteNamespace(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDBAdmin).WithTimeout(serverConfig_.HttpWriteTimeout());
	string nsName = urldecode2(ctx.request->urlParams[1]);

	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	auto status = db.DropNamespace(nsName);
	if (!status.ok()) {
		http::HttpStatus httpStatus(status);

		return jsonStatus(ctx, httpStatus);
	}

	return jsonStatus(ctx);
}

int HTTPServer::TruncateNamespace(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDBAdmin).WithTimeout(serverConfig_.HttpWriteTimeout());
	string nsName = urldecode2(ctx.request->urlParams[1]);

	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	auto status = db.TruncateNamespace(nsName);
	if (!status.ok()) {
		http::HttpStatus httpStatus(status);

		return jsonStatus(ctx, httpStatus);
	}

	return jsonStatus(ctx);
}

int HTTPServer::RenameNamespace(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDBAdmin);	 // No timeout for this operation, because it doesn't supported by cluster
	string srcNsName = urldecode2(ctx.request->urlParams[1]);
	string dstNsName = urldecode2(ctx.request->urlParams[2]);

	if (srcNsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	if (dstNsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "New namespace name is not specified"));
	}

	auto status = db.RenameNamespace(srcNsName, dstNsName);
	if (!status.ok()) {
		http::HttpStatus httpStatus(status);

		return jsonStatus(ctx, httpStatus);
	}

	return jsonStatus(ctx);
}

int HTTPServer::GetItems(http::Context &ctx) {
	std::string_view sharding = ctx.request->params.Get("sharding");

	auto db = sharding == "off"sv
				  ? getDB(ctx, kRoleDataRead).WithTimeout(serverConfig_.HttpReadTimeout).WithShardId(ShardingKeyType::ProxyOff, false)
				  : getDB(ctx, kRoleDataRead).WithTimeout(serverConfig_.HttpReadTimeout);

	string nsName = urldecode2(ctx.request->urlParams[1]);

	std::string_view limitParam = ctx.request->params.Get("limit");
	std::string_view offsetParam = ctx.request->params.Get("offset");
	std::string_view sortField = ctx.request->params.Get("sort_field");
	std::string_view sortOrder = ctx.request->params.Get("sort_order");

	string filterParam = urldecode2(ctx.request->params.Get("filter"));
	string fields = urldecode2(ctx.request->params.Get("fields"));

	if (nsName.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}
	if (fields.empty()) {
		fields = "*";
	}

	reindexer::WrSerializer querySer;
	querySer << "SELECT " << fields << " FROM " << nsName;
	if (filterParam.length()) {
		querySer << " WHERE " << filterParam;
	}
	if (sortField.length()) {
		querySer << " ORDER BY " << sortField;

		if (sortOrder == "desc") {
			querySer << " DESC";
		} else if ((sortOrder.size() > 0) && (sortOrder != "asc")) {
			return status(ctx, http::HttpStatus(http::StatusBadRequest, "Invalid `sort_order` parameter"));
		}
	}
	if (limitParam.length()) {
		querySer << " LIMIT " << prepareLimit(limitParam);
	}
	if (offsetParam.length()) {
		querySer << " OFFSET " << prepareOffset(offsetParam);
	}

	reindexer::Query q;

	q.FromSQL(querySer.Slice());
	q.ReqTotal();

	reindexer::QueryResults res;
	auto ret = db.Select(q, res);
	if (!ret.ok()) {
		return status(ctx, http::HttpStatus(http::StatusInternalServerError, ret.what()));
	}

	return queryResults(ctx, res);
}

int HTTPServer::DeleteItems(http::Context &ctx) { return modifyItems(ctx, ModeDelete); }
int HTTPServer::PutItems(http::Context &ctx) { return modifyItems(ctx, ModeUpdate); }
int HTTPServer::PostItems(http::Context &ctx) { return modifyItems(ctx, ModeInsert); }
int HTTPServer::PatchItems(http::Context &ctx) { return modifyItems(ctx, ModeUpsert); }

int HTTPServer::GetMetaList(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDataRead).WithTimeout(serverConfig_.HttpReadTimeout);
	const string nsName = urldecode2(ctx.request->urlParams[1]);
	if (!nsName.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	enum SortOrder { Desc = -1, NoSort = 0, Asc = 1 } sortDirection = NoSort;
	bool withValues = false;

	std::string_view sortOrder = ctx.request->params.Get("sort_order");
	if (sortOrder == "asc") {
		sortDirection = Asc;
	} else if (sortOrder == "desc") {
		sortDirection = Desc;
	} else if (sortOrder.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Invalid `sort_order` parameter"));
	}

	std::string_view withValParam = ctx.request->params.Get("with_values");
	if (withValParam == "true") {
		withValues = true;
	} else if (withValParam == "false") {
		withValues = false;
	} else if (withValParam.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Invalid `with_values` parameter"));
	}
	std::string_view limitParam = ctx.request->params.Get("limit");
	std::string_view offsetParam = ctx.request->params.Get("offset");
	unsigned limit = prepareLimit(limitParam, 0);
	unsigned offset = prepareOffset(offsetParam, 0);

	std::vector<std::string> keys;
	const Error err = db.EnumMeta(nsName, keys);
	if (!err.ok()) {
		return jsonStatus(ctx, http::HttpStatus(err));
	}
	if (sortDirection == Asc) {
		std::sort(keys.begin(), keys.end());
	} else if (sortDirection == Desc) {
		std::sort(keys.begin(), keys.end(), std::greater<std::string>());
	}
	auto keysIt = keys.begin();
	auto keysEnd = keys.end();
	if (offset >= keys.size()) {
		keysEnd = keysIt;
	} else {
		std::advance(keysIt, offset);
	}
	if (limit > 0 && limit + offset < keys.size()) {
		keysEnd = keysIt;
		std::advance(keysEnd, limit);
	}

	WrSerializer ser(ctx.writer->GetChunk());
	JsonBuilder builder(ser);
	builder.Put("total_items", keys.size());
	JsonBuilder arrNode = builder.Array("meta");
	for (; keysIt != keysEnd; ++keysIt) {
		auto objNode = arrNode.Object();
		objNode.Put("key", *keysIt);
		if (withValues) {
			std::string value;
			const Error err = db.GetMeta(nsName, *keysIt, value);
			if (!err.ok()) return jsonStatus(ctx, http::HttpStatus(err));
			objNode.Put("value", escapeString(value));
		}
		objNode.End();
	}
	arrNode.End();
	builder.End();

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::GetMetaByKey(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDataRead).WithTimeout(serverConfig_.HttpReadTimeout);
	const string nsName = urldecode2(ctx.request->urlParams[1]);
	const string key = urldecode2(ctx.request->urlParams[2]);
	if (!nsName.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}
	std::string value;
	const Error err = db.GetMeta(nsName, key, value);
	if (!err.ok()) {
		return jsonStatus(ctx, http::HttpStatus(err));
	}
	WrSerializer ser(ctx.writer->GetChunk());
	JsonBuilder builder(ser);
	builder.Put("key", escapeString(key));
	builder.Put("value", escapeString(value));
	builder.End();
	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::PutMetaByKey(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDataWrite).WithTimeout(serverConfig_.HttpWriteTimeout());
	const string nsName = urldecode2(ctx.request->urlParams[1]);
	if (!nsName.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}
	try {
		gason::JsonParser parser;
		std::string body = ctx.body->Read();
		auto root = parser.Parse(giftStr(body));
		std::string key = root["key"].As<string>();
		std::string value = root["value"].As<string>();
		const Error err = db.PutMeta(nsName, key, unescapeString(value));
		if (!err.ok()) {
			return jsonStatus(ctx, http::HttpStatus(err));
		}
	} catch (const gason::Exception &ex) {
		return jsonStatus(ctx, http::HttpStatus(Error(errParseJson, "Meta: %s", ex.what())));
	}
	return jsonStatus(ctx);
}

int HTTPServer::GetIndexes(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDataRead).WithTimeout(serverConfig_.HttpReadTimeout);

	string nsName = urldecode2(ctx.request->urlParams[1]);

	if (!nsName.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	vector<reindexer::NamespaceDef> nsDefs;
	db.EnumNamespaces(nsDefs, EnumNamespacesOpts().WithFilter(nsName));

	if (nsDefs.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusNotFound, "Namespace is not found"));
	}

	WrSerializer ser(ctx.writer->GetChunk());
	{
		JsonBuilder builder(ser);
		builder.Put("total_items", nsDefs[0].indexes.size());
		auto arrNode = builder.Array("items");
		for (auto &idxDef : nsDefs[0].indexes) {
			arrNode.Raw(nullptr, "");
			idxDef.GetJSON(ser);
		}
	}
	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::PostIndex(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDBAdmin).WithTimeout(serverConfig_.HttpWriteTimeout());

	string nsName = urldecode2(ctx.request->urlParams[1]);
	if (!nsName.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	string json = ctx.body->Read();
	string newIdxName = getNameFromJson(json);

	vector<reindexer::NamespaceDef> nsDefs;
	db.EnumNamespaces(nsDefs, EnumNamespacesOpts().WithFilter(nsName));

	reindexer::IndexDef idxDef;
	idxDef.FromJSON(giftStr(json));

	if (!nsDefs.empty()) {
		auto &indexes = nsDefs[0].indexes;
		auto foundIndexIt =
			std::find_if(indexes.begin(), indexes.end(), [&newIdxName](const IndexDef &idx) { return idx.name_ == newIdxName; });
		if (foundIndexIt != indexes.end()) {
			return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Index already exists"));
		}
	}

	auto status = db.AddIndex(nsName, idxDef);
	if (!status.ok()) {
		http::HttpStatus httpStatus(status);

		return jsonStatus(ctx, httpStatus);
	}

	return jsonStatus(ctx);
}

int HTTPServer::PutIndex(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDBAdmin).WithTimeout(serverConfig_.HttpWriteTimeout());

	string nsName = urldecode2(ctx.request->urlParams[1]);
	if (!nsName.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	reindexer::IndexDef idxDef;
	std::string body = ctx.body->Read();
	idxDef.FromJSON(giftStr(body));

	auto status = db.UpdateIndex(nsName, idxDef);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	return jsonStatus(ctx);
}

int HTTPServer::PutSchema(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDBAdmin).WithTimeout(serverConfig_.HttpWriteTimeout());

	string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	auto status = db.SetSchema(nsName, ctx.body->Read());
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	return jsonStatus(ctx);
}

int HTTPServer::GetSchema(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDataRead).WithTimeout(serverConfig_.HttpReadTimeout);

	string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	std::string schema;
	auto status = db.GetSchema(nsName, JsonSchemaType, schema);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	return ctx.JSON(http::StatusOK, schema.length() ? schema : "{}"sv);
}

int HTTPServer::GetProtobufSchema(http::Context &ctx) {
	Reindexer db = getDB(ctx, kRoleDataRead).WithTimeout(serverConfig_.HttpReadTimeout);

	std::vector<string> nses;
	for (auto &p : ctx.request->params) {
		if (p.name == "ns"sv || p.name == "ns[]"sv) {
			nses.emplace_back(urldecode2(p.val));
		}
	}

	WrSerializer ser;
	Error err = db.GetProtobufSchema(ser, nses);
	if (!err.ok()) return jsonStatus(ctx, http::HttpStatus(err));

	return ctx.String(http::StatusOK, ser.Slice());
}

int HTTPServer::DeleteIndex(http::Context &ctx) {
	auto db = getDB(ctx, kRoleDBAdmin).WithTimeout(serverConfig_.HttpWriteTimeout());

	string nsName = urldecode2(ctx.request->urlParams[1]);
	IndexDef idef(urldecode2(ctx.request->urlParams[2]));

	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	if (idef.name_.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Index is not specified"));
	}

	auto status = db.DropIndex(nsName, idef);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	return jsonStatus(ctx);
}

int HTTPServer::Check(http::Context &ctx) {
	WrSerializer ser(ctx.writer->GetChunk());
	{
		JsonBuilder builder(ser);
		builder.Put("version", REINDEX_VERSION);

		size_t startTs = std::chrono::duration_cast<std::chrono::seconds>(startTs_.time_since_epoch()).count();
		size_t uptime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - startTs_).count();
		builder.Put("start_time", startTs);
		builder.Put("uptime", uptime);
		builder.Put("rpc_address", serverConfig_.RPCAddr);
		builder.Put("http_address", serverConfig_.HTTPAddr);
		builder.Put("storage_path", serverConfig_.StoragePath);
		builder.Put("rpc_log", serverConfig_.RpcLog);
		builder.Put("http_log", serverConfig_.HttpLog);
		builder.Put("log_level", serverConfig_.LogLevel);
		builder.Put("core_log", serverConfig_.CoreLog);
		builder.Put("server_log", serverConfig_.ServerLog);

#if REINDEX_WITH_JEMALLOC
		if (alloc_ext::JEMallocIsAvailable()) {
			size_t val = 0, val1 = 1, sz = sizeof(size_t);

			uint64_t epoch = 1;
			sz = sizeof(epoch);
			alloc_ext::mallctl("epoch", &epoch, &sz, &epoch, sz);

			alloc_ext::mallctl("stats.resident", &val, &sz, NULL, 0);
			builder.Put("heap_size", val);

			alloc_ext::mallctl("stats.allocated", &val, &sz, NULL, 0);
			builder.Put("current_allocated_bytes", val);

			alloc_ext::mallctl("stats.active", &val1, &sz, NULL, 0);
			builder.Put("pageheap_free", val1 - val);

			alloc_ext::mallctl("stats.retained", &val, &sz, NULL, 0);
			builder.Put("pageheap_unmapped", val);
		}
#elif REINDEX_WITH_GPERFTOOLS
		if (alloc_ext::TCMallocIsAvailable()) {
			size_t val = 0;
			alloc_ext::instance()->GetNumericProperty("generic.current_allocated_bytes", &val);
			builder.Put("current_allocated_bytes", val);

			alloc_ext::instance()->GetNumericProperty("generic.heap_size", &val);
			builder.Put("heap_size", val);

			alloc_ext::instance()->GetNumericProperty("tcmalloc.pageheap_free_bytes", &val);
			builder.Put("pageheap_free", val);

			alloc_ext::instance()->GetNumericProperty("tcmalloc.pageheap_unmapped_bytes", &val);
			builder.Put("pageheap_unmapped", val);
		}
#endif
	}

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}
int HTTPServer::DocHandler(http::Context &ctx) {
	string path(ctx.request->path.substr(1));

	bool endsWithSlash = (path.length() > 0 && path.back() == '/');
	if (endsWithSlash) {
		path.pop_back();
	}

	if (path == "" || path == "/") {
		return ctx.Redirect("face/");
	}

	web web(webRoot_);

	auto stat = web.stat(path);
	if (stat.fstatus == fs::StatFile) {
		return web.file(ctx, http::StatusOK, path, stat.isGzip);
	}

	if (stat.fstatus == fs::StatDir && !endsWithSlash) {
		return ctx.Redirect(path + "/");
	}

	for (; path.length() > 0;) {
		string file = fs::JoinPath(path, "index.html");
		const auto pathStatus = web.stat(file);
		if (pathStatus.fstatus == fs::StatFile) {
			return web.file(ctx, http::StatusOK, file, pathStatus.isGzip);
		}

		auto pos = path.find_last_of('/');
		if (pos == string::npos) break;

		path = path.erase(pos);
	}

	return NotFoundHandler(ctx);
}

int HTTPServer::NotFoundHandler(http::Context &ctx) {
	http::HttpStatus httpStatus(http::StatusNotFound, "Not found");

	return jsonStatus(ctx, httpStatus);
}

bool HTTPServer::Start(const string &addr, ev::dynamic_loop &loop) {
	router_.NotFound<HTTPServer, &HTTPServer::NotFoundHandler>(this);

	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/swagger", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/swagger/*", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/face", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/face/*", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/facestaging", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/facestaging/*", this);

	router_.GET<HTTPServer, &HTTPServer::Check>("/api/v1/check", this);

	router_.GET<HTTPServer, &HTTPServer::GetSQLQuery>("/api/v1/db/:db/query", this);
	router_.POST<HTTPServer, &HTTPServer::PostQuery>("/api/v1/db/:db/query", this);
	router_.POST<HTTPServer, &HTTPServer::PostSQLQuery>("/api/v1/db/:db/sqlquery", this);
	router_.POST<HTTPServer, &HTTPServer::UpdateQuery>("/api/v1/db/:db/dslquery", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteQuery>("/api/v1/db/:db/query", this);
	// router_.PUT<HTTPServer, &HTTPServer::UpdateQuery>("/api/v1/db/:db/query", this); TODO: implement dsl parsing
	router_.GET<HTTPServer, &HTTPServer::GetSQLSuggest>("/api/v1/db/:db/suggest", this);

	router_.GET<HTTPServer, &HTTPServer::GetProtobufSchema>("/api/v1/db/:db/protobuf_schema", this);

	router_.GET<HTTPServer, &HTTPServer::GetDatabases>("/api/v1/db", this);
	router_.POST<HTTPServer, &HTTPServer::PostDatabase>("/api/v1/db", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteDatabase>("/api/v1/db/:db", this);

	router_.GET<HTTPServer, &HTTPServer::GetNamespaces>("/api/v1/db/:db/namespaces", this);
	router_.GET<HTTPServer, &HTTPServer::GetNamespace>("/api/v1/db/:db/namespaces/:ns", this);
	router_.POST<HTTPServer, &HTTPServer::PostNamespace>("/api/v1/db/:db/namespaces", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteNamespace>("/api/v1/db/:db/namespaces/:ns", this);
	router_.DELETE<HTTPServer, &HTTPServer::TruncateNamespace>("/api/v1/db/:db/namespaces/:ns/truncate", this);
	router_.GET<HTTPServer, &HTTPServer::RenameNamespace>("/api/v1/db/:db/namespaces/:ns/rename/:nns", this);

	router_.GET<HTTPServer, &HTTPServer::GetItems>("/api/v1/db/:db/namespaces/:ns/items", this);
	router_.PUT<HTTPServer, &HTTPServer::PutItems>("/api/v1/db/:db/namespaces/:ns/items", this);
	router_.POST<HTTPServer, &HTTPServer::PostItems>("/api/v1/db/:db/namespaces/:ns/items", this);
	router_.PATCH<HTTPServer, &HTTPServer::PatchItems>("/api/v1/db/:db/namespaces/:ns/items", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteItems>("/api/v1/db/:db/namespaces/:ns/items", this);

	router_.GET<HTTPServer, &HTTPServer::GetIndexes>("/api/v1/db/:db/namespaces/:ns/indexes", this);
	router_.POST<HTTPServer, &HTTPServer::PostIndex>("/api/v1/db/:db/namespaces/:ns/indexes", this);
	router_.PUT<HTTPServer, &HTTPServer::PutIndex>("/api/v1/db/:db/namespaces/:ns/indexes", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteIndex>("/api/v1/db/:db/namespaces/:ns/indexes/:idx", this);
	router_.PUT<HTTPServer, &HTTPServer::PutSchema>("/api/v1/db/:db/namespaces/:ns/schema", this);
	router_.GET<HTTPServer, &HTTPServer::GetSchema>("/api/v1/db/:db/namespaces/:ns/schema", this);

	router_.GET<HTTPServer, &HTTPServer::GetMetaList>("/api/v1/db/:db/namespaces/:ns/metalist", this);
	router_.GET<HTTPServer, &HTTPServer::GetMetaByKey>("/api/v1/db/:db/namespaces/:ns/metabykey/:key", this);
	router_.PUT<HTTPServer, &HTTPServer::PutMetaByKey>("/api/v1/db/:db/namespaces/:ns/metabykey", this);

	router_.POST<HTTPServer, &HTTPServer::BeginTx>("/api/v1/db/:db/namespaces/:ns/transactions/begin", this);
	router_.POST<HTTPServer, &HTTPServer::CommitTx>("/api/v1/db/:db/transactions/:tx/commit", this);
	router_.POST<HTTPServer, &HTTPServer::RollbackTx>("/api/v1/db/:db/transactions/:tx/rollback", this);
	router_.PUT<HTTPServer, &HTTPServer::PutItemsTx>("/api/v1/db/:db/transactions/:tx/items", this);
	router_.POST<HTTPServer, &HTTPServer::PostItemsTx>("/api/v1/db/:db/transactions/:tx/items", this);
	router_.PATCH<HTTPServer, &HTTPServer::PatchItemsTx>("/api/v1/db/:db/transactions/:tx/items", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteItemsTx>("/api/v1/db/:db/transactions/:tx/items", this);
	router_.GET<HTTPServer, &HTTPServer::GetSQLQueryTx>("/api/v1/db/:db/transactions/:tx/query", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteQueryTx>("/api/v1/db/:db/transactions/:tx/query", this);

	router_.OnResponse(this, &HTTPServer::OnResponse);
	router_.Middleware<HTTPServer, &HTTPServer::CheckAuth>(this);

	if (logger_) {
		router_.Logger<HTTPServer, &HTTPServer::Logger>(this);
	}

	if (serverConfig_.DebugPprof) {
		pprof_.Attach(router_);
	}
	if (prometheus_) {
		prometheus_->Attach(router_);
	}

	if (serverConfig_.HttpThreadingMode == ServerConfig::kDedicatedThreading) {
		listener_.reset(new ForkedListener(loop, http::ServerConnection::NewFactory(router_, serverConfig_.MaxHttpReqSize)));
	} else {
		listener_.reset(new Listener(loop, http::ServerConnection::NewFactory(router_, serverConfig_.MaxHttpReqSize)));
	}
	deadlineChecker_.set<HTTPServer, &HTTPServer::deadlineTimerCb>(this);
	deadlineChecker_.set(loop);
	deadlineChecker_.start(std::chrono::duration_cast<std::chrono::seconds>(kTxDeadlineCheckPeriod).count(),
						   std::chrono::duration_cast<std::chrono::seconds>(kTxDeadlineCheckPeriod).count());

	return listener_->Bind(addr);
}

Error HTTPServer::modifyItem(Reindexer &db, string &nsName, Item &item, ItemModifyMode mode) {
	Error status;
	switch (mode) {
		case ModeUpsert:
			status = db.Upsert(nsName, item);
			break;
		case ModeDelete:
			status = db.Delete(nsName, item);
			break;
		case ModeInsert:
			status = db.Insert(nsName, item);
			break;
		case ModeUpdate:
			status = db.Update(nsName, item);
			break;
	}
	return status;
}

Error HTTPServer::modifyItem(Reindexer &db, string &nsName, Item &item, QueryResults &qr, ItemModifyMode mode) {
	Error status;
	switch (mode) {
		case ModeUpsert:
			status = db.Upsert(nsName, item, qr);
			break;
		case ModeDelete:
			status = db.Delete(nsName, item, qr);
			break;
		case ModeInsert:
			status = db.Insert(nsName, item, qr);
			break;
		case ModeUpdate:
			status = db.Update(nsName, item, qr);
			break;
	}
	return status;
}

int HTTPServer::modifyItemsJSON(http::Context &ctx, string &nsName, vector<string> &&precepts, ItemModifyMode mode) {
	auto db = getDB(ctx, kRoleDataWrite).WithTimeout(serverConfig_.HttpWriteTimeout());
	string itemJson = ctx.body->Read();
	int cnt = 0;
	vector<string> updatedItems;

	if (itemJson.size()) {
		char *jsonPtr = &itemJson[0];
		size_t jsonLeft = itemJson.size();
		while (jsonPtr && *jsonPtr) {
			Item item = db.NewItem(nsName);
			if (!item.Status().ok()) {
				return jsonStatus(ctx, http::HttpStatus(item.Status()));
			}
			char *prevPtr = jsonPtr;
			auto str = std::string_view(jsonPtr, jsonLeft);
			if (jsonPtr != &itemJson[0] && isBlank(str)) {
				break;
			}
			auto status = item.Unsafe().FromJSON(str, &jsonPtr, false);	 // TODO: for mode == ModeDelete deserialize PK and sharding key
																		 // only
			jsonLeft -= (jsonPtr - prevPtr);

			if (!status.ok()) {
				return jsonStatus(ctx, http::HttpStatus(status));
			}

			item.SetPrecepts(precepts);
			status = modifyItem(db, nsName, item, mode);

			if (!status.ok()) {
				return jsonStatus(ctx, http::HttpStatus(status));
			}

			if (item.GetID() != -1) {
				++cnt;
				if (!precepts.empty()) updatedItems.push_back(string(item.GetJSON()));
			}
		}
		db.Commit(nsName);
	}

	WrSerializer ser(ctx.writer->GetChunk());
	JsonBuilder builder(ser);
	builder.Put(kParamUpdated, cnt);
	builder.Put(kParamSuccess, true);
	if (!precepts.empty()) {
		auto itemsArray = builder.Array(kParamItems);
		for (const string &item : updatedItems) itemsArray.Raw(nullptr, item);
		itemsArray.End();
	}
	builder.End();

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::modifyItemsMsgPack(http::Context &ctx, string &nsName, vector<std::string> &&precepts, ItemModifyMode mode) {
	QueryResults qr;
	int totalItems = 0;

	auto db = getDB(ctx, kRoleDataWrite).WithTimeout(serverConfig_.HttpWriteTimeout());
	string sbuffer = ctx.body->Read();

	size_t length = sbuffer.size();
	size_t offset = 0;

	while (offset < length) {
		Item item = db.NewItem(nsName);
		if (!item.Status().ok()) return msgpackStatus(ctx, http::HttpStatus(item.Status()));

		Error status = item.FromMsgPack(std::string_view(sbuffer.data(), sbuffer.size()), offset);
		if (!status.ok()) return msgpackStatus(ctx, http::HttpStatus(status));

		item.SetPrecepts(precepts);
		if (!precepts.empty()) {
			status = modifyItem(db, nsName, item, qr, mode);
		} else {
			status = modifyItem(db, nsName, item, mode);
		}
		if (!status.ok()) return msgpackStatus(ctx, http::HttpStatus(status));

		if (item.GetID() != -1) {
			++totalItems;
		}
	}

	WrSerializer wrSer(ctx.writer->GetChunk());
	MsgPackBuilder msgpackBuilder(wrSer, ObjType::TypeObject, precepts.empty() ? 2 : 3);
	msgpackBuilder.Put(kParamUpdated, totalItems);
	msgpackBuilder.Put(kParamSuccess, true);
	if (!precepts.empty()) {
		auto itemsArray = msgpackBuilder.Array(kParamItems, qr.Count());
		for (auto it : qr) {
			it.GetMsgPack(wrSer, false);
		}
		itemsArray.End();
	}

	return ctx.MSGPACK(http::StatusOK, wrSer.DetachChunk());
}

int HTTPServer::modifyItemsProtobuf(http::Context &ctx, string &nsName, vector<std::string> &&precepts, ItemModifyMode mode) {
	WrSerializer wrSer(ctx.writer->GetChunk());
	ProtobufBuilder builder(&wrSer);

	auto sendResponse = [&](int items, const Error &err) {
		if (err.ok()) {
			builder.Put(kProtoModifyResultsFields.at(kParamUpdated), int(items));
			builder.Put(kProtoModifyResultsFields.at(kParamSuccess), err.ok());
		} else {
			builder.Put(kProtoErrorResultsFields.at(kParamDescription), err.what());
			builder.Put(kProtoErrorResultsFields.at(kParamResponseCode), err.code());
		}
		return ctx.Protobuf(reindexer::net::http::HttpStatus::errCodeToHttpStatus(err.code()), wrSer.DetachChunk());
	};

	auto db = getDB(ctx, kRoleDataWrite).WithTimeout(serverConfig_.HttpWriteTimeout());
	Item item = db.NewItem(nsName);
	if (!item.Status().ok()) return sendResponse(0, item.Status());

	string sbuffer = ctx.body->Read();
	Error status = item.FromProtobuf(std::string_view(sbuffer.data(), sbuffer.size()));
	if (!status.ok()) return sendResponse(0, status);

	const bool hasPrecepts = !precepts.empty();
	item.SetPrecepts(std::move(precepts));
	status = modifyItem(db, nsName, item, mode);
	if (!status.ok()) return sendResponse(0, item.Status());

	int totalItems = 0;
	if (item.GetID() != -1) {
		if (hasPrecepts) {
			auto object = builder.Object(kProtoModifyResultsFields.at(kParamItems));
			status = item.GetProtobuf(wrSer);
			object.End();
		}
		++totalItems;
	}

	return sendResponse(totalItems, item.Status());
}

int HTTPServer::modifyItemsTxJSON(http::Context &ctx, Transaction &tx, vector<string> &&precepts, ItemModifyMode mode) {
	string itemJson = ctx.body->Read();

	if (itemJson.size()) {
		char *jsonPtr = &itemJson[0];
		size_t jsonLeft = itemJson.size();
		while (jsonPtr && *jsonPtr) {
			Item item = tx.NewItem();
			if (!item.Status().ok()) {
				http::HttpStatus httpStatus(item.Status());
				return jsonStatus(ctx, httpStatus);
			}
			char *prevPtr = jsonPtr;
			auto str = std::string_view(jsonPtr, jsonLeft);
			if (jsonPtr != &itemJson[0] && isBlank(str)) {
				break;
			}
			auto status = item.FromJSON(std::string_view(jsonPtr, jsonLeft), &jsonPtr,
										false);	 // TODO: for mode == ModeDelete deserialize PK and sharding key only
			jsonLeft -= (jsonPtr - prevPtr);
			if (!status.ok()) {
				http::HttpStatus httpStatus(status);
				return jsonStatus(ctx, httpStatus);
			}
			item.SetPrecepts(precepts);
			auto err = tx.Modify(std::move(item), mode);
			if (!err.ok()) {
				return jsonStatus(ctx, http::HttpStatus(err));
			}
		}
	}

	return jsonStatus(ctx);
}

int HTTPServer::modifyItemsTxMsgPack(http::Context &ctx, Transaction &tx, vector<string> &&precepts, ItemModifyMode mode) {
	string sbuffer = ctx.body->Read();
	size_t length = sbuffer.size();
	size_t offset = 0;

	while (offset < length) {
		Item item = tx.NewItem();
		if (!item.Status().ok()) return msgpackStatus(ctx, http::HttpStatus(item.Status()));

		Error status = item.FromMsgPack(std::string_view(sbuffer.data(), sbuffer.size()), offset);
		if (!status.ok()) return msgpackStatus(ctx, http::HttpStatus(status));

		item.SetPrecepts(precepts);
		auto err = tx.Modify(std::move(item), mode);
		if (!err.ok()) {
			return jsonStatus(ctx, http::HttpStatus(err));
		}
	}

	return msgpackStatus(ctx);
}

int HTTPServer::modifyItems(http::Context &ctx, ItemModifyMode mode) {
	string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	vector<string> precepts;
	for (auto &p : ctx.request->params) {
		if ((p.name == "precepts"sv) || (p.name == "precepts[]"sv)) {
			precepts.emplace_back(urldecode2(p.val));
		}
	}

	const auto format = ctx.request->params.Get("format");
	if (format == "msgpack"sv) {
		return modifyItemsMsgPack(ctx, nsName, std::move(precepts), mode);
	} else if (format == "protobuf"sv) {
		return modifyItemsProtobuf(ctx, nsName, std::move(precepts), mode);
	} else {
		return modifyItemsJSON(ctx, nsName, std::move(precepts), mode);
	}
}

int HTTPServer::modifyItemsTx(http::Context &ctx, ItemModifyMode mode) {
	std::string dbName;
	auto db = getDB(ctx, kRoleDataWrite, &dbName).WithTimeout(serverConfig_.HttpWriteTimeout());
	std::string txId = urldecode2(ctx.request->urlParams[1]);
	if (txId.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Tx ID is not specified"));
	}

	std::vector<std::string> precepts;
	for (auto &p : ctx.request->params) {
		if ((p.name == "precepts"sv) || (p.name == "precepts[]"sv)) {
			precepts.push_back(urldecode2(p.val));
		}
	}

	const auto format = ctx.request->params.Get("format"sv);
	auto tx = getTx(dbName, txId);
	return format == "msgpack"sv ? modifyItemsTxMsgPack(ctx, *tx, std::move(precepts), mode)
								 : modifyItemsTxJSON(ctx, *tx, std::move(precepts), mode);
}

int HTTPServer::queryResultsJSON(http::Context &ctx, reindexer::QueryResults &res, bool isWALQuery, bool isQueryResults, unsigned limit,
								 unsigned offset, bool withColumns, int width) {
	WrSerializer wrSer(ctx.writer->GetChunk());
	JsonBuilder builder(wrSer);

	auto iarray = builder.Array(kParamItems);
	auto it = res.begin() + offset;
	std::vector<std::string> jsonData;
	if (withColumns) {
		size_t size = res.Count();
		if (limit > offset && limit - offset < size) {
			size = limit - offset;
		}
		jsonData.reserve(size);
	}
	WrSerializer itemSer;
	auto cjsonViewer = [this, &res, &ctx](std::string_view cjson) {
		auto item = getDB(ctx, kRoleDataRead).WithTimeout(serverConfig_.HttpReadTimeout).NewItem(res.GetNamespaces()[0]);
		item.FromCJSON(cjson);
		return string(item.GetJSON());
	};

	for (size_t i = 0; it != res.end() && i < limit; ++i, ++it) {
		if (!isWALQuery) {
			iarray.Raw(nullptr, "");
			if (withColumns) {
				itemSer.Reset();
				it.GetJSON(itemSer, false);
				jsonData.emplace_back(itemSer.Slice());
				wrSer.Write(itemSer.Slice());
			} else {
				it.GetJSON(wrSer, false);
			}
		} else {
			auto obj = iarray.Object(nullptr);
			obj.Put(kParamLsn, int64_t(it.GetLSN()));
			if (!it.IsRaw()) {
				iarray.Raw(kParamItem, "");
				if (withColumns) {
					itemSer.Reset();
					it.GetJSON(itemSer, false);
					jsonData.emplace_back(itemSer.Slice());
					wrSer.Write(itemSer.Slice());
				} else {
					it.GetJSON(wrSer, false);
				}
			} else {
				reindexer::WALRecord rec(it.GetRaw());
				rec.GetJSON(obj, cjsonViewer);
			}
		}

		if (i == offset) wrSer.Reserve(wrSer.Len() * (std::min(limit, unsigned(res.Count() - offset)) + 1));
	}
	iarray.End();

	auto &aggs = res.GetAggregationResults();
	if (!aggs.empty()) {
		auto arrNode = builder.Array(kParamAggregations);
		for (auto &agg : aggs) {
			arrNode.Raw(nullptr, "");
			agg.GetJSON(wrSer);
		}
	}

	queryResultParams(builder, res, std::move(jsonData), isWALQuery, isQueryResults, limit, withColumns, width);
	builder.End();

	return ctx.JSON(http::StatusOK, wrSer.DetachChunk());
}

int HTTPServer::queryResultsMsgPack(http::Context &ctx, reindexer::QueryResults &res, bool isWALQuery, bool isQueryResults, unsigned limit,
									unsigned offset, bool withColumns, int width) {
	int paramsToSend = 3;
	bool withTotalItems = (!isQueryResults || limit != kDefaultLimit);
	if (!res.GetAggregationResults().empty()) ++paramsToSend;
	if (!res.GetExplainResults().empty()) ++paramsToSend;
	if (withTotalItems) ++paramsToSend;
	if (withColumns) ++paramsToSend;
	if (isQueryResults && res.TotalCount()) {
		if (limit == kDefaultLimit) ++paramsToSend;
		++paramsToSend;
	}

	WrSerializer wrSer(ctx.writer->GetChunk());
	MsgPackBuilder msgpackBuilder(wrSer, ObjType::TypeObject, paramsToSend);

	WrSerializer itemSer;
	std::vector<std::string> jsonData;
	if (withColumns) {
		size_t size = res.Count();
		if (limit > offset && limit - offset < size) {
			size = limit - offset;
		}
		jsonData.reserve(size);
	}
	auto itemsArray = msgpackBuilder.Array(kParamItems, std::min(size_t(limit), size_t(res.Count() - offset)));
	auto it = res.begin() + offset;
	for (size_t i = 0; it != res.end() && i < limit; ++i, ++it) {
		it.GetMsgPack(wrSer, false);
		if (withColumns) {
			itemSer.Reset();
			it.GetJSON(itemSer, false);
			jsonData.emplace_back(itemSer.Slice());
		}
	}
	itemsArray.End();

	if (!res.GetAggregationResults().empty()) {
		auto &aggs = res.GetAggregationResults();
		auto aggregationsArray = msgpackBuilder.Array(kParamAggregations, aggs.size());
		for (auto &agg : aggs) {
			agg.GetMsgPack(wrSer);
		}
	}

	queryResultParams(msgpackBuilder, res, std::move(jsonData), isWALQuery, isQueryResults, limit, withColumns, width);
	msgpackBuilder.End();

	return ctx.MSGPACK(http::StatusOK, wrSer.DetachChunk());
}

int HTTPServer::queryResultsProtobuf(http::Context &ctx, reindexer::QueryResults &res, bool isWALQuery, bool isQueryResults, unsigned limit,
									 unsigned offset, bool withColumns, int width) {
	WrSerializer wrSer(ctx.writer->GetChunk());
	ProtobufBuilder protobufBuilder(&wrSer);

	int itemsField = kProtoQueryResultsFields.at(kParamItems);
	auto &lres = res.ToLocalQr();
	WrSerializer itemSer;
	std::vector<std::string> jsonData;
	if (withColumns) {
		size_t size = res.Count();
		if (limit > offset && limit - offset < size) {
			size = limit - offset;
		}
		jsonData.reserve(size);
	}
	for (size_t i = offset; i < lres.Count() && i < offset + limit; i++) {
		auto item = protobufBuilder.Object(itemsField);
		auto it = lres[i];
		auto i1 = item.Object(lres.getNsNumber(it.GetItemRef().Nsid()) + 1);
		it.GetProtobuf(wrSer, false);
		i1.End();
		item.End();
		if (withColumns) {
			itemSer.Reset();
			it.GetJSON(itemSer, false);
			jsonData.emplace_back(itemSer.Slice());
		}
	}

	int aggregationField = kProtoQueryResultsFields.at(kParamAggregations);
	for (auto &agg : res.GetAggregationResults()) {
		auto aggregation = protobufBuilder.Object(aggregationField);
		agg.GetProtobuf(wrSer);
		aggregation.End();
	}

	int nsField = kProtoQueryResultsFields.at(kParamNamespaces);
	h_vector<std::string_view, 1> namespaces(res.GetNamespaces());
	for (auto ns : namespaces) {
		protobufBuilder.Put(nsField, ns);
	}

	protobufBuilder.Put(kProtoQueryResultsFields.at(kParamCacheEnabled), res.IsCacheEnabled() && !isWALQuery);

	if (!res.GetExplainResults().empty()) {
		protobufBuilder.Put(kProtoQueryResultsFields.at(kParamExplain), res.GetExplainResults());
	}

	if (!isQueryResults || limit != kDefaultLimit) {
		protobufBuilder.Put(kProtoQueryResultsFields.at(kParamTotalItems),
							isQueryResults ? static_cast<int64_t>(res.Count()) : static_cast<int64_t>(res.TotalCount()));
	}

	if (isQueryResults && res.TotalCount()) {
		protobufBuilder.Put(kProtoQueryResultsFields.at(kParamQueryTotalItems), int(res.TotalCount()));
		if (limit == kDefaultLimit) {
			protobufBuilder.Put(kProtoQueryResultsFields.at(kParamTotalItems), int(res.TotalCount()));
		}
	}

	if (withColumns) {
		reindexer::TableCalculator tableCalculator(std::move(jsonData), width);
		auto &header = tableCalculator.GetHeader();
		auto &columnsSettings = tableCalculator.GetColumnsSettings();
		for (auto it = header.begin(); it != header.end(); ++it) {
			ColumnData &data = columnsSettings[*it];
			auto parameteresObj = protobufBuilder.Object(kProtoQueryResultsFields.at(kParamColumns));
			parameteresObj.Put(kProtoColumnsFields.at(kParamName), *it);
			parameteresObj.Put(kProtoColumnsFields.at(kParamWidthPercents), data.widthTerminalPercentage);
			parameteresObj.Put(kProtoColumnsFields.at(kParamMaxChars), data.maxWidthCh);
			parameteresObj.Put(kProtoColumnsFields.at(kParamWidthChars), data.widthCh);
			parameteresObj.End();
		}
	}

	protobufBuilder.End();
	return ctx.Protobuf(http::StatusOK, wrSer.DetachChunk());
}

template <typename Builder>
void HTTPServer::queryResultParams(Builder &builder, reindexer::QueryResults &res, std::vector<std::string> &&jsonData, bool isWALQuery,
								   bool isQueryResults, unsigned limit, bool withColumns, int width) {
	h_vector<std::string_view, 1> namespaces(res.GetNamespaces());
	auto namespacesArray = builder.Array(kParamNamespaces, namespaces.size());
	for (auto ns : namespaces) {
		namespacesArray.Put(nullptr, ns);
	}
	namespacesArray.End();

	builder.Put(kParamCacheEnabled, res.IsCacheEnabled() && !isWALQuery);

	if (!res.GetExplainResults().empty()) {
		builder.Json(kParamExplain, res.GetExplainResults());
	}

	if (!isQueryResults || limit != kDefaultLimit) {
		builder.Put(kParamTotalItems, isQueryResults ? static_cast<int64_t>(res.Count()) : static_cast<int64_t>(res.TotalCount()));
	}

	if (isQueryResults && res.TotalCount()) {
		builder.Put(kParamQueryTotalItems, int(res.TotalCount()));
		if (limit == kDefaultLimit) {
			builder.Put(kParamTotalItems, int(res.TotalCount()));
		}
	}

	if (withColumns) {
		reindexer::TableCalculator tableCalculator(std::move(jsonData), width);
		auto &header = tableCalculator.GetHeader();
		auto &columnsSettings = tableCalculator.GetColumnsSettings();
		auto headerArray = builder.Array(kParamColumns, header.size());
		for (auto it = header.begin(); it != header.end(); ++it) {
			ColumnData &data = columnsSettings[*it];
			auto parameteresObj = headerArray.Object(nullptr, 4);
			parameteresObj.Put(kParamName, *it);
			parameteresObj.Put(kParamWidthPercents, data.widthTerminalPercentage);
			parameteresObj.Put(kParamMaxChars, data.maxWidthCh);
			parameteresObj.Put(kParamWidthChars, data.widthCh);
		}
	}
}

int HTTPServer::queryResults(http::Context &ctx, reindexer::QueryResults &res, bool isWALQuery, bool isQueryResults, unsigned limit,
							 unsigned offset) {
	std::string_view widthParam = ctx.request->params.Get("width"sv);
	int width = stoi(widthParam);

	std::string_view format = ctx.request->params.Get("format");
	std::string_view withColumnsParam = ctx.request->params.Get("with_columns");
	bool withColumns = ((withColumnsParam == "1") && (width > 0)) ? true : false;

	if (format == "msgpack"sv) {
		return queryResultsMsgPack(ctx, res, isWALQuery, isQueryResults, limit, offset, withColumns, width);
	} else if (format == "protobuf"sv) {
		return queryResultsProtobuf(ctx, res, isWALQuery, isQueryResults, limit, offset, withColumns, width);
	} else {
		return queryResultsJSON(ctx, res, isWALQuery, isQueryResults, limit, offset, withColumns, width);
	}
}

int HTTPServer::status(http::Context &ctx, const http::HttpStatus &status) {
	std::string_view format = ctx.request->params.Get("format"sv);
	if (format == "msgpack"sv) {
		return msgpackStatus(ctx, status);
	} else if (format == "protobuf") {
		return protobufStatus(ctx, status);
	} else {
		return jsonStatus(ctx, status);
	}
}

int HTTPServer::msgpackStatus(http::Context &ctx, const http::HttpStatus &status) {
	WrSerializer wrSer(ctx.writer->GetChunk());
	MsgPackBuilder msgpackBuilder(wrSer, ObjType::TypeObject, 3);
	msgpackBuilder.Put(kParamSuccess, status.code == http::StatusOK);
	msgpackBuilder.Put(kParamResponseCode, status.code);
	msgpackBuilder.Put(kParamDescription, status.what);
	msgpackBuilder.End();
	return ctx.MSGPACK(status.code, wrSer.DetachChunk());
}

int HTTPServer::jsonStatus(http::Context &ctx, const http::HttpStatus &status) {
	WrSerializer ser(ctx.writer->GetChunk());
	JsonBuilder builder(ser);
	builder.Put(kParamSuccess, status.code == http::StatusOK);
	builder.Put(kParamResponseCode, int(status.code));
	builder.Put(kParamDescription, status.what);
	builder.End();
	return ctx.JSON(status.code, ser.DetachChunk());
}

int HTTPServer::protobufStatus(http::Context &ctx, const http::HttpStatus &status) {
	WrSerializer ser(ctx.writer->GetChunk());
	ProtobufBuilder builder(&ser);
	builder.Put(kProtoErrorResultsFields.at(kParamSuccess), status.code == http::StatusOK);
	builder.Put(kProtoErrorResultsFields.at(kParamResponseCode), int(status.code));
	builder.Put(kProtoErrorResultsFields.at(kParamDescription), status.what);
	builder.End();
	return ctx.Protobuf(status.code, ser.DetachChunk());
}

unsigned HTTPServer::prepareLimit(std::string_view limitParam, int limitDefault) {
	int limit = limitDefault;

	if (limitParam.length()) {
		limit = stoi(limitParam);
		if (limit < 0) limit = 0;
	}

	return static_cast<unsigned>(limit);
}

unsigned HTTPServer::prepareOffset(std::string_view offsetParam, int offsetDefault) {
	int offset = offsetDefault;

	if (offsetParam.length()) {
		offset = stoi(offsetParam);
		if (offset < 0) offset = 0;
	}

	return static_cast<unsigned>(offset);
}

int HTTPServer::modifyQueryTxImpl(http::Context &ctx, const std::string &dbName, std::string_view txId, Query &q) {
	reindexer::QueryResults res;
	auto tx = getTx(dbName, txId);
	if (!q.mergeQueries_.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Merged subqueries are not allowed inside TX"));
	}
	if (!q.joinQueries_.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Joined subqueries are not allowed inside TX"));
	}
	auto err = tx->Modify(std::move(q));
	return status(ctx, http::HttpStatus(err));
}

Reindexer HTTPServer::getDB(http::Context &ctx, UserRole role, string *dbNameOut) {
	(void)ctx;
	Reindexer *db = nullptr;

	string dbName(urldecode2(ctx.request->urlParams[0]));

	AuthContext dummyCtx;

	AuthContext *actx = &dummyCtx;
	if (!dbMgr_.IsNoSecurity()) {
		auto clientData = dynamic_cast<HTTPClientData *>(ctx.clientData.get());
		assert(clientData);
		actx = &clientData->auth;
	}

	auto status = dbMgr_.OpenDatabase(dbName, *actx, false);
	if (!status.ok()) {
		throw http::HttpStatus(status);
	}
	if (dbNameOut) {
		*dbNameOut = std::move(dbName);
	}

	status = actx->GetDB(role, &db);
	if (!status.ok()) {
		throw http::HttpStatus(status);
	}
	assert(db);
	return db->NeedTraceActivity() ? db->WithActivityTracer(ctx.request->clientAddr, ctx.request->headers.Get("User-Agent")) : *db;
}

string HTTPServer::getNameFromJson(std::string_view json) {
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);
		return root["name"].As<string>();
	} catch (const gason::Exception &ex) {
		throw Error(errParseJson, "getNameFromJson: %s", ex.what());
	}
}

std::shared_ptr<Transaction> HTTPServer::getTx(const string &dbName, std::string_view txId) {
	std::lock_guard<std::mutex> lck(txMtx_);
	auto found = txMap_.find(txId);
	if (found == txMap_.end()) {
		throw http::HttpStatus(Error(errNotFound, "Invalid tx id"sv));
	}
	if (!iequals(found.value().dbName, dbName)) {
		throw http::HttpStatus(Error(errLogic, "Unexpected database name for this tx"sv));
	}
	found.value().txDeadline = TxDeadlineClock::now() + serverConfig_.TxIdleTimeout;
	return found.value().tx;
}

std::string HTTPServer::addTx(std::string dbName, Transaction &&tx) {
	auto ts = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch());
	std::string txId = randStringAlph(kTxIdLen) + "_" + std::to_string(ts.count());
	TxInfo txInfo;
	txInfo.tx = std::make_shared<Transaction>(std::move(tx));
	txInfo.dbName = std::move(dbName);
	txInfo.txDeadline = TxDeadlineClock::now() + serverConfig_.TxIdleTimeout;
	std::lock_guard<std::mutex> lck(txMtx_);
	auto result = txMap_.try_emplace(txId, std::move(txInfo));
	if (!result.second) {
		throw Error(errLogic, "Tx id conflict");
	}
	return txId;
}

void HTTPServer::removeTx(const string &dbName, std::string_view txId) {
	std::lock_guard<std::mutex> lck(txMtx_);
	auto found = txMap_.find(txId);
	if (found == txMap_.end() || !iequals(found.value().dbName, dbName)) {
		throw Error(errNotFound, "Invalid tx id");
	}
	txMap_.erase(found);
}

void HTTPServer::removeExpiredTx() {
	auto now = TxDeadlineClock::now();
	std::lock_guard<std::mutex> lck(txMtx_);
	for (auto it = txMap_.begin(); it != txMap_.end();) {
		if (it->second.txDeadline <= now) {
			auto ctx = MakeSystemAuthContext();
			auto status = dbMgr_.OpenDatabase(it->second.dbName, ctx, false);
			if (status.ok()) {
				reindexer::Reindexer *db = nullptr;
				status = ctx.GetDB(kRoleSystem, &db);
				if (db) {
					logger_.info("Rollback tx {} on idle deadline", it->first);
					db->RollBackTransaction(*it->second.tx);
				}
			}
			it = txMap_.erase(it);
		} else {
			++it;
		}
	}
}

int HTTPServer::CheckAuth(http::Context &ctx) {
	(void)ctx;
	if (dbMgr_.IsNoSecurity()) {
		return 0;
	}

	std::string_view authHeader = ctx.request->headers.Get("authorization");

	if (authHeader.length() < 6) {
		ctx.writer->SetHeader({"WWW-Authenticate"sv, "Basic realm=\"reindexer\""sv});
		ctx.String(http::StatusUnauthorized, "Forbidden"sv);
		return -1;
	}

	h_vector<char, 128> credVec(authHeader.length());
	char *credBuf = &credVec.front();
	Base64decode(credBuf, authHeader.data() + 6);
	char *password = strchr(credBuf, ':');
	if (password != nullptr) *password++ = 0;

	AuthContext auth(credBuf, password ? password : "");
	auto status = dbMgr_.Login("", auth);
	if (!status.ok()) {
		ctx.writer->SetHeader({"WWW-Authenticate"sv, "Basic realm=\"reindexer\""sv});
		ctx.String(http::StatusUnauthorized, status.what());
		return -1;
	}

	std::unique_ptr<HTTPClientData> clientData(new HTTPClientData);
	clientData->auth = auth;
	ctx.clientData = std::move(clientData);
	return 0;
}

int HTTPServer::BeginTx(http::Context &ctx) {
	string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	std::string dbName;
	auto db = getDB(ctx, kRoleDataWrite, &dbName);
	auto tx = db.NewTransaction(nsName);
	if (!tx.Status().ok()) {
		return status(ctx, http::HttpStatus(tx.Status()));
	}
	auto txId = addTx(std::move(dbName), std::move(tx));

	WrSerializer ser;
	if (ctx.request->params.Get("format"sv) == "msgpack"sv) {
		MsgPackBuilder builder(ser, ObjType::TypeObject, 1);
		builder.Put(kTxId, txId);
		builder.End();
		return ctx.MSGPACK(http::StatusOK, ser.DetachChunk());
	} else {
		JsonBuilder builder(ser);
		builder.Put(kTxId, txId);
		builder.End();
		return ctx.JSON(http::StatusOK, ser.DetachChunk());
	}
}

int HTTPServer::CommitTx(http::Context &ctx) {
	string txId = urldecode2(ctx.request->urlParams[1]);
	if (txId.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Tx ID is not specified"));
	}

	string dbName;
	auto db = getDB(ctx, kRoleDataWrite, &dbName);
	auto tx = getTx(dbName, txId);
	QueryResults qr;
	auto ret = db.CommitTransaction(*tx, qr);
	if (!ret.ok()) {
		return status(ctx, http::HttpStatus(http::StatusInternalServerError, ret.what()));
	}
	removeTx(dbName, txId);
	return queryResults(ctx, qr);
}

int HTTPServer::RollbackTx(http::Context &ctx) {
	string txId = urldecode2(ctx.request->urlParams[1]);
	if (txId.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Tx ID is not specified"));
	}

	string dbName;
	auto db = getDB(ctx, kRoleDataWrite, &dbName);
	auto tx = getTx(dbName, txId);
	QueryResults qr;
	auto ret = db.RollBackTransaction(*tx);
	removeTx(dbName, txId);
	if (!ret.ok()) {
		return status(ctx, http::HttpStatus(ret));
	}
	return status(ctx);
}

int HTTPServer::PostItemsTx(http::Context &ctx) { return modifyItemsTx(ctx, ModeInsert); }

int HTTPServer::PutItemsTx(http::Context &ctx) { return modifyItemsTx(ctx, ModeUpdate); }

int HTTPServer::PatchItemsTx(http::Context &ctx) { return modifyItemsTx(ctx, ModeUpsert); }

int HTTPServer::DeleteItemsTx(http::Context &ctx) { return modifyItemsTx(ctx, ModeDelete); }

int HTTPServer::GetSQLQueryTx(http::Context &ctx) {
	string dbName;
	auto db = getDB(ctx, kRoleDataRead, &dbName);
	string txId = urldecode2(ctx.request->urlParams[1]);
	if (txId.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Tx ID is not specified"));
	}
	reindexer::QueryResults res;
	string sqlQuery = urldecode2(ctx.request->params.Get("q"));
	if (sqlQuery.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Missed `q` parameter"));
	}

	try {
		Query q;
		q.FromSQL(sqlQuery);
		switch (q.type_) {
			case QueryDelete:
			case QueryUpdate:
				return modifyQueryTxImpl(ctx, dbName, txId, q);
			default:
				return status(ctx, http::HttpStatus(http::StatusInternalServerError, "Transactions support update/delete queries only"));
		}
	} catch (const Error &e) {
		return status(ctx, http::HttpStatus(e));
	}
}

int HTTPServer::DeleteQueryTx(http::Context &ctx) {
	string dbName;
	auto db = getDB(ctx, kRoleDataWrite, &dbName);
	string dsl = ctx.body->Read();

	reindexer::Query q;
	auto ret = q.FromJSON(dsl);
	if (!ret.ok()) {
		return jsonStatus(ctx, http::HttpStatus(ret));
	}
	reindexer::QueryResults res;
	string txId = urldecode2(ctx.request->urlParams[1]);
	if (txId.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Tx ID is not specified"));
	}

	q.type_ = QueryDelete;
	return modifyQueryTxImpl(ctx, dbName, txId, q);
}

void HTTPServer::Logger(http::Context &ctx) {
	HandlerStat statDiff = HandlerStat() - ctx.stat.allocStat;
	auto clientData = reinterpret_cast<HTTPClientData *>(ctx.clientData.get());
	if (serverConfig_.DebugAllocs) {
		logger_.info("{} - {} {} {} {} {} {}us | allocs: {}, allocated: {} byte(s)", ctx.request->clientAddr,
					 clientData ? clientData->auth.Login() : "", ctx.request->method, ctx.request->uri, ctx.writer->RespCode(),
					 ctx.writer->Written(), statDiff.GetTimeElapsed(), statDiff.GetAllocsCnt(), statDiff.GetAllocsBytes());
	} else {
		logger_.info("{} - {} {} {} {} {} {}us", ctx.request->clientAddr, clientData ? clientData->auth.Login() : "", ctx.request->method,
					 ctx.request->uri, ctx.writer->RespCode(), ctx.writer->Written(), statDiff.GetTimeElapsed());
	}
}

void HTTPServer::OnResponse(http::Context &ctx) {
	if (statsWatcher_) {
		std::string dbName = "<unknown>";
		if (nullptr != ctx.request && !ctx.request->urlParams.empty() && 0 == ctx.request->path.find("/api/v1/db/"sv)) {
			dbName = urldecode2(ctx.request->urlParams[0]);
		}
		statsWatcher_->OnInputTraffic(dbName, statsSourceName(), ctx.stat.sizeStat.reqSizeBytes);
		statsWatcher_->OnOutputTraffic(dbName, statsSourceName(), ctx.stat.sizeStat.respSizeBytes);
	}
}

}  // namespace reindexer_server
