#pragma once

#include <gtest/gtest.h>
#include <thread>
#include "rpcserver_fake.h"

#include "client/reindexer.h"
#include "server/server.h"

#include "client/cororeindexer.h"

class RPCClientTestApi : public ::testing::Test {
public:
	RPCClientTestApi() {}
	virtual ~RPCClientTestApi() {}

protected:
	class CancelRdxContext : public reindexer::IRdxCancelContext {
	public:
		reindexer::CancelType GetCancelType() const noexcept override {
			return canceld_.load() ? reindexer::CancelType::Explicit : reindexer::CancelType::None;
		}
		bool IsCancelable() const noexcept override { return true; }
		void Cancel() { canceld_ = true; }

	private:
		std::atomic<bool> canceld_ = {false};
	};

	class TestServer {
	public:
		TestServer(const RPCServerConfig& conf) : terminate_(false), serverIsReady_(false), conf_(conf) {}
		void Start(const string& addr, Error errOnLogin = Error());
		void Stop();
		const string& GetDsn() const { return dsn_; }
		RPCServerStatus Status() const { return server_->Status(); }

	private:
		unique_ptr<RPCServerFake> server_;
		unique_ptr<std::thread> serverThread_;
		net::ev::dynamic_loop loop_;
		net::ev::async stop_;
		atomic<bool> terminate_;
		atomic<bool> serverIsReady_;
		string dsn_;
		RPCServerConfig conf_;
	};

	void SetUp() {}
	void TearDown() { StopAllServers(); }

public:
	void StartDefaultRealServer();
	void AddFakeServer(const string& addr = kDefaultRPCServerAddr, const RPCServerConfig& conf = RPCServerConfig());
	void AddRealServer(const std::string& dbPath, const string& addr = kDefaultRPCServerAddr, uint16_t httpPort = kDefaultHttpPort,
					   uint16_t clusterPort = kDefaultClusterPort);
	void StartServer(const string& addr = kDefaultRPCServerAddr, Error errOnLogin = Error());
	void StopServer(const string& addr = kDefaultRPCServerAddr);
	bool CheckIfFakeServerConnected(const string& addr = kDefaultRPCServerAddr);
	void StopAllServers();
	client::Item CreateItem(reindexer::client::Reindexer& rx, std::string_view nsName, int id);
	client::Item CreateItem(reindexer::client::CoroReindexer& rx, std::string_view nsName, int id);
	void CreateNamespace(reindexer::client::Reindexer& rx, std::string_view nsName);
	void CreateNamespace(reindexer::client::CoroReindexer& rx, std::string_view nsName);
	void FillData(reindexer::client::Reindexer& rx, std::string_view nsName, int from, int count);
	void FillData(reindexer::client::CoroReindexer& rx, std::string_view nsName, int from, int count);

	static const std::string kDbPrefix;
	static const uint16_t kDefaultRPCPort = 25673;
	static const std::string kDefaultRPCServerAddr;
	static const uint16_t kDefaultHttpPort = 33333;
	static const uint16_t kDefaultClusterPort = 33833;

private:
	struct ServerData {
		ServerData() { server.reset(new reindexer_server::Server()); }

		std::unique_ptr<reindexer_server::Server> server;
		std::unique_ptr<std::thread> serverThread;
	};

	unordered_map<string, unique_ptr<TestServer>> fakeServers_;
	unordered_map<string, ServerData> realServers_;
};
