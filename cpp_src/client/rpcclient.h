#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include "client/internalrdxcontext.h"
#include "client/item.h"
#include "client/namespace.h"
#include "client/queryresults.h"
#include "client/reindexerconfig.h"
#include "client/transaction.h"
#include "core/keyvalue/p_string.h"
#include "core/namespacedef.h"
#include "core/query/query.h"
#include "estl/fast_hash_map.h"
#include "estl/shared_mutex.h"
#include "net/cproto/clientconnection.h"
#include "tools/errors.h"
#include "urlparser/urlparser.h"

namespace reindexer {

namespace client {

using std::string;
using std::atomic_bool;
using std::shared_ptr;
using std::chrono::seconds;

using namespace net;
class RPCClient {
public:
	typedef std::function<void(const Error &err)> Completion;
	RPCClient(const ReindexerConfig &config);
	RPCClient(const RPCClient &) = delete;
	RPCClient(RPCClient &&) = delete;
	RPCClient &operator=(const RPCClient &) = delete;
	RPCClient &operator=(RPCClient &&) = delete;
	~RPCClient();

	Error Connect(const string &dsn, const client::ConnectOpts &opts);
	Error Connect(const vector<pair<string, client::ConnectOpts>> &connectData);
	Error Stop();

	Error OpenNamespace(std::string_view nsName, const InternalRdxContext &ctx,
						const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing());
	Error AddNamespace(const NamespaceDef &nsDef, const InternalRdxContext &ctx);
	Error CloseNamespace(std::string_view nsName, const InternalRdxContext &ctx);
	Error DropNamespace(std::string_view nsName, const InternalRdxContext &ctx);
	Error TruncateNamespace(std::string_view nsName, const InternalRdxContext &ctx);
	Error RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx);
	Error AddIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error UpdateIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error DropIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext &ctx);
	Error GetSchema(std::string_view nsName, int format, std::string &schema, const InternalRdxContext &ctx);
	Error EnumNamespaces(vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx);
	Error EnumDatabases(vector<string> &dbList, const InternalRdxContext &ctx);
	Error Insert(std::string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Update(std::string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Upsert(std::string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Delete(std::string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Delete(const Query &query, QueryResults &result, const InternalRdxContext &ctx);
	Error Update(const Query &query, QueryResults &result, const InternalRdxContext &ctx);
	Error Select(std::string_view query, QueryResults &result, const InternalRdxContext &ctx, cproto::ClientConnection *conn = nullptr) {
		return selectImpl(query, result, conn, config_.RequestTimeout, ctx);
	}
	Error Select(const Query &query, QueryResults &result, const InternalRdxContext &ctx, cproto::ClientConnection *conn = nullptr) {
		return selectImpl(query, result, conn, config_.RequestTimeout, ctx);
	}
	Error Commit(std::string_view nsName);
	Item NewItem(std::string_view nsName);
	Error GetMeta(std::string_view nsName, const string &key, string &data, const InternalRdxContext &ctx);
	Error PutMeta(std::string_view nsName, const string &key, std::string_view data, const InternalRdxContext &ctx);
	Error EnumMeta(std::string_view nsName, vector<string> &keys, const InternalRdxContext &ctx);
	Error GetSqlSuggestions(std::string_view query, int pos, std::vector<std::string> &suggests);
	Error Status();

	Transaction NewTransaction(std::string_view nsName, const InternalRdxContext &ctx);
	Error CommitTransaction(Transaction &tr, const InternalRdxContext &ctx);
	Error RollBackTransaction(Transaction &tr, const InternalRdxContext &ctx);

protected:
	struct worker {
		worker() : running(false) {}
		ev::dynamic_loop loop_;
		std::thread thread_;
		ev::async stop_;
		atomic_bool running;
	};
	Error selectImpl(std::string_view query, QueryResults &result, cproto::ClientConnection *, seconds netTimeout,
					 const InternalRdxContext &ctx);
	Error selectImpl(const Query &query, QueryResults &result, cproto::ClientConnection *, seconds netTimeout,
					 const InternalRdxContext &ctx);
	Error modifyItem(std::string_view nsName, Item &item, int mode, seconds netTimeout, const InternalRdxContext &ctx);
	Error modifyItemAsync(std::string_view nsName, Item *item, int mode, cproto::ClientConnection *, seconds netTimeout,
						  const InternalRdxContext &ctx);
	Namespace *getNamespace(std::string_view nsName);
	Error startWorkers();
	Error addConnectEntry(const string &dsn, const client::ConnectOpts &opts, size_t idx);
	void run(size_t thIdx);
	bool onConnectionFail(int failedDsnIndex);

	net::cproto::ClientConnection *getConn();
	cproto::CommandParams mkCommand(cproto::CmdCode cmd, const InternalRdxContext *ctx = nullptr) const noexcept;
	static cproto::CommandParams mkCommand(cproto::CmdCode cmd, seconds reqTimeout, const InternalRdxContext *ctx) noexcept;

	std::vector<std::unique_ptr<net::cproto::ClientConnection>> connections_;

	fast_hash_map<string, Namespace::Ptr, nocase_hash_str, nocase_equal_str> namespaces_;

	shared_timed_mutex nsMutex_;
	std::vector<worker> workers_;
	std::atomic<unsigned> curConnIdx_;
	ReindexerConfig config_;
	cproto::ClientConnection::ConnectData connectData_;
};

void vec2pack(const h_vector<int32_t, 4> &vec, WrSerializer &ser);

}  // namespace client
}  // namespace reindexer
