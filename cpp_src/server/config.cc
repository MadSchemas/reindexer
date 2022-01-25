#include "config.h"

#include "args/args.hpp"
#include "core/storage/storagefactory.h"
#include "tools/fsops.h"
#include "yaml/yaml.h"

namespace reindexer_server {

constexpr auto kDefaultClusterHttpWriteTimeout = std::chrono::seconds(20);

void ServerConfig::Reset() {
	args_.clear();
	WebRoot.clear();
	StorageEngine = "leveldb";
	HTTPAddr = "0.0.0.0:9088";
	RPCAddr = "0.0.0.0:6534";
	GRPCAddr = "0.0.0.0:16534";
	RPCThreadingMode = kSharedThreading;
	HttpThreadingMode = kSharedThreading;
	LogLevel = "info";
	ServerLog = "stdout";
	CoreLog = "stdout";
	HttpLog = "stdout";
	RpcLog = "stdout";
#ifndef _WIN32
	StoragePath = "/tmp/reindex";
	UserName.clear();
	DaemonPidFile = "reindexer.pid";
	Daemonize = false;
#else
	StoragePath = "\\reindexer";
	InstallSvc = false;
	RemoveSvc = false;
	SvcMode = false;
#endif
	StartWithErrors = false;
	EnableSecurity = false;
	DebugPprof = false;
	EnablePrometheus = false;
	PrometheusCollectPeriod = std::chrono::milliseconds(1000);
	DebugAllocs = false;
	Autorepair = false;
	EnableConnectionsStats = true;
	TxIdleTimeout = std::chrono::seconds(600);
	HttpReadTimeout = std::chrono::seconds(0);
	httpWriteTimeout_ = std::chrono::seconds(0);
	hasCustomHttpWriteTimeout_ = false;
	enableCluster_ = false;
	MaxUpdatesSize = 1024 * 1024 * 1024;
	EnableGRPC = false;
	MaxHttpReqSize = 2 * 1024 * 1024;
}

const string ServerConfig::kDedicatedThreading = "dedicated";
const string ServerConfig::kSharedThreading = "shared";
const string ServerConfig::kPoolThreading = "pool";

reindexer::Error ServerConfig::ParseYaml(const std::string &yaml) {
	Error err;
	Yaml::Node root;
	try {
		Yaml::Parse(root, yaml);
		err = fromYaml(root);
	} catch (const Yaml::Exception &ex) {
		err = Error(errParams, "Error with config string. Reason: '%s'", ex.Message());
	}
	return err;
}

Error ServerConfig::ParseFile(const std::string &filePath) {
	Error err;
	Yaml::Node root;
	try {
		Yaml::Parse(root, filePath.c_str());
		err = fromYaml(root);
	} catch (const Yaml::Exception &ex) {
		err = Error(errParams, "Error with config file '%s'. Reason: %s", filePath, ex.Message());
	}
	return err;
}

Error ServerConfig::ParseCmd(int argc, char *argv[]) {
	using reindexer::fs::GetDirPath;
#ifndef LINK_RESOURCES
	WebRoot = GetDirPath(argv[0]);
#endif
	args_.assign(argv, argv + argc);

	args::ArgumentParser parser("reindexer server");
	args::HelpFlag help(parser, "help", "Show this message", {'h', "help"});
	args::Flag securityF(parser, "", "Enable per-user security", {"security"});
	args::ValueFlag<string> configF(parser, "CONFIG", "Path to reindexer config file", {'c', "config"}, args::Options::Single);
	args::Flag startWithErrorsF(parser, "", "Allow to start reindexer with DB's load erros", {"startwitherrors"});

	args::Group dbGroup(parser, "Database options");
	args::ValueFlag<string> storageF(dbGroup, "PATH", "path to 'reindexer' storage", {'s', "db"}, StoragePath, args::Options::Single);
	auto availableStorageTypes = reindexer::datastorage::StorageFactory::getAvailableTypes();
	std::string availabledStorages;
	for (const auto &type : availableStorageTypes) {
		if (!availabledStorages.empty()) {
			availabledStorages.append(", ");
		}
		availabledStorages.append("'" + reindexer::datastorage::StorageTypeToString(type) + "'");
	}
	args::ValueFlag<string> storageEngineF(dbGroup, "NAME", "'reindexer' storage engine (" + availabledStorages + ")", {'e', "engine"},
										   StorageEngine, args::Options::Single);
	args::Flag autorepairF(dbGroup, "", "Enable autorepair for storages after unexpected shutdowns", {"autorepair"});

	args::Group netGroup(parser, "Network options");
	args::ValueFlag<string> httpAddrF(netGroup, "PORT", "http listen host:port", {'p', "httpaddr"}, HTTPAddr, args::Options::Single);
	args::ValueFlag<string> rpcAddrF(netGroup, "RPORT", "RPC listen host:port", {'r', "rpcaddr"}, RPCAddr, args::Options::Single);
	args::Flag enableClusterF(netGroup, "",
							  "Enable RAFT-cluster support. This will also implicitly enable 'dedicated' threading mode for RPC-server",
							  {"enable-cluster"});
	args::ValueFlag<string> rpcThreadingModeF(netGroup, "RTHREADING", "RPC connections threading mode: shared or dedicated",
											  {'X', "rpc-threading"}, RPCThreadingMode, args::Options::Single);
	args::ValueFlag<string> httpThreadingModeF(netGroup, "HTHREADING", "HTTP connections threading mode: shared or dedicated",
											   {"http-threading"}, HttpThreadingMode, args::Options::Single);
	args::ValueFlag<size_t> MaxHttpReqSizeF(
		netGroup, "", "Max HTTP request size in bytes. Default value is 2 MB. 0 is 'unlimited', hovewer, stream mode is not supported",
		{"max-http-req"}, MaxHttpReqSize, args::Options::Single);
#ifdef WITH_GRPC
	args::ValueFlag<string> grpcAddrF(netGroup, "GPORT", "GRPC listen host:port", {'g', "grpcaddr"}, RPCAddr, args::Options::Single);
	args::Flag grpcF(netGroup, "", "Enable gRpc service", {"grpc"});
#endif
	args::ValueFlag<string> webRootF(netGroup, "PATH", "web root. This path if set overrides linked-in resources", {'w', "webroot"},
									 WebRoot, args::Options::Single);
	args::ValueFlag<int> httpReadTimeoutF(netGroup, "", "timeout (s) for HTTP read operations (i.e. selects, get meta and others)",
										  {"http-read-timeout"}, args::Options::Single);
	args::ValueFlag<int> httpWriteTimeoutF(netGroup, "", "timeout (s) for HTTP write operations (i.e. selects, get meta and others)",
										   {"http-write-timeout"}, args::Options::Single);
	args::ValueFlag<size_t> maxUpdatesSizeF(
		netGroup, "", "Maximum cached updates size for async or cluster replication. Min value is 1000000 bytes. '0' means unlimited",
		{"updatessize"}, MaxUpdatesSize, args::Options::Single);
	args::Flag pprofF(netGroup, "", "Enable pprof http handler", {'f', "pprof"});
	args::ValueFlag<int> txIdleTimeoutF(netGroup, "", "http transactions idle timeout (s)", {"tx-idle-timeout"}, TxIdleTimeout.count(),
										args::Options::Single);

	args::Group metricsGroup(parser, "Metrics options");
	args::Flag prometheusF(metricsGroup, "", "Enable prometheus handler", {"prometheus"});
	args::ValueFlag<int> prometheusPeriodF(metricsGroup, "", "Prometheus stats collect period (ms)", {"prometheus-period"},
										   PrometheusCollectPeriod.count(), args::Options::Single);
	args::Flag clientsConnectionsStatF(metricsGroup, "", "Enable client connection statistic", {"clientsstats"});

	args::Group logGroup(parser, "Logging options");
	args::ValueFlag<string> logLevelF(logGroup, "", "log level (none, warning, error, info, trace)", {'l', "loglevel"}, LogLevel,
									  args::Options::Single);
	args::ValueFlag<string> serverLogF(logGroup, "", "Server log file", {"serverlog"}, ServerLog, args::Options::Single);
	args::ValueFlag<string> coreLogF(logGroup, "", "Core log file", {"corelog"}, CoreLog, args::Options::Single);
	args::ValueFlag<string> httpLogF(logGroup, "", "Http log file", {"httplog"}, HttpLog, args::Options::Single);
	args::ValueFlag<string> rpcLogF(logGroup, "", "Rpc log file", {"rpclog"}, RpcLog, args::Options::Single);
	args::Flag logAllocsF(netGroup, "", "Log operations allocs statistics", {'a', "allocs"});

#ifndef _WIN32
	args::Group unixDaemonGroup(parser, "Unix daemon options");
	args::ValueFlag<string> userF(unixDaemonGroup, "USER", "System user name", {'u', "user"}, UserName, args::Options::Single);
	args::Flag daemonizeF(unixDaemonGroup, "", "Run in daemon mode", {'d', "daemonize"});
	args::ValueFlag<string> daemonPidFileF(unixDaemonGroup, "", "Custom daemon pid file", {"pidfile"}, DaemonPidFile,
										   args::Options::Single);
#else
	args::Group winSvcGroup(parser, "Windows service options");
	args::Flag installF(winSvcGroup, "", "Install reindexer windows service", {"install"});
	args::Flag removeF(winSvcGroup, "", "Remove reindexer windows service", {"remove"});
	args::Flag serviceF(winSvcGroup, "", "Run in service mode", {"service"});
#endif

	try {
		parser.ParseCLI(argc, argv);
	} catch (const args::Help &) {
		return Error(errLogic, parser.Help());
	} catch (const args::Error &e) {
		return Error(errParams, "%s\n%s", e.what(), parser.Help());
	}

	if (configF) {
		auto err = ParseFile(args::get(configF));
		if (!err.ok()) return err;
	}

	if (storageF) StoragePath = args::get(storageF);
	if (storageEngineF) StorageEngine = args::get(storageEngineF);
	if (startWithErrorsF) StartWithErrors = args::get(startWithErrorsF);
	if (autorepairF) Autorepair = args::get(autorepairF);
	if (logLevelF) LogLevel = args::get(logLevelF);
	if (httpAddrF) HTTPAddr = args::get(httpAddrF);
	if (rpcAddrF) RPCAddr = args::get(rpcAddrF);
	if (enableClusterF) SetEnableCluster(args::get(enableClusterF));
	if (rpcThreadingModeF) RPCThreadingMode = args::get(rpcThreadingModeF);
	if (httpThreadingModeF) HttpThreadingMode = args::get(httpThreadingModeF);
	if (webRootF) WebRoot = args::get(webRootF);
	if (MaxHttpReqSizeF) MaxHttpReqSize = args::get(MaxHttpReqSizeF);
#ifndef _WIN32
	if (userF) UserName = args::get(userF);
	if (daemonizeF) Daemonize = args::get(daemonizeF);
	if (daemonPidFileF) DaemonPidFile = args::get(daemonPidFileF);
#else
	if (installF) InstallSvc = args::get(installF);
	if (removeF) RemoveSvc = args::get(removeF);
	if (serviceF) SvcMode = args::get(serviceF);

#endif
	if (securityF) EnableSecurity = args::get(securityF);
#ifdef WITH_GRPC
	if (grpcF) EnableGRPC = args::get(grpcF);
	if (grpcAddrF) GRPCAddr = args::get(grpcAddrF);
#endif
	if (serverLogF) ServerLog = args::get(serverLogF);
	if (coreLogF) CoreLog = args::get(coreLogF);
	if (httpLogF) HttpLog = args::get(httpLogF);
	if (rpcLogF) RpcLog = args::get(rpcLogF);
	if (pprofF) DebugPprof = args::get(pprofF);
	if (prometheusF) EnablePrometheus = args::get(prometheusF);
	if (prometheusPeriodF) PrometheusCollectPeriod = std::chrono::milliseconds(args::get(prometheusPeriodF));
	if (clientsConnectionsStatF) EnableConnectionsStats = args::get(clientsConnectionsStatF);
	if (httpReadTimeoutF) HttpReadTimeout = std::chrono::seconds(args::get(httpReadTimeoutF));
	if (httpWriteTimeoutF) SetHttpWriteTimeout(std::chrono::seconds(args::get(httpWriteTimeoutF)));
	if (logAllocsF) DebugAllocs = args::get(logAllocsF);
	if (txIdleTimeoutF) TxIdleTimeout = std::chrono::seconds(args::get(txIdleTimeoutF));
	if (maxUpdatesSizeF) MaxUpdatesSize = args::get(maxUpdatesSizeF);

	return Error();
}

void ServerConfig::SetEnableCluster(bool val) noexcept {
	enableCluster_ = val;
	if (!hasCustomHttpWriteTimeout_ && enableCluster_) {
		httpWriteTimeout_ = kDefaultClusterHttpWriteTimeout;
	}
}

void ServerConfig::SetHttpWriteTimeout(std::chrono::seconds val) noexcept {
	hasCustomHttpWriteTimeout_ = true;
	httpWriteTimeout_ = val;
}

reindexer::Error ServerConfig::fromYaml(Yaml::Node &root) {
	try {
		StoragePath = root["storage"]["path"].As<std::string>(StoragePath);
		StorageEngine = root["storage"]["engine"].As<std::string>(StorageEngine);
		StartWithErrors = root["storage"]["startwitherrors"].As<bool>(StartWithErrors);
		Autorepair = root["storage"]["autorepair"].As<bool>(Autorepair);
		LogLevel = root["logger"]["loglevel"].As<std::string>(LogLevel);
		ServerLog = root["logger"]["serverlog"].As<std::string>(ServerLog);
		CoreLog = root["logger"]["corelog"].As<std::string>(CoreLog);
		HttpLog = root["logger"]["httplog"].As<std::string>(HttpLog);
		RpcLog = root["logger"]["rpclog"].As<std::string>(RpcLog);
		HTTPAddr = root["net"]["httpaddr"].As<std::string>(HTTPAddr);
		RPCAddr = root["net"]["rpcaddr"].As<std::string>(RPCAddr);
		const auto enableCluster = root["net"]["enable_cluster"].As<bool>(EnableCluster());
		SetEnableCluster(enableCluster);
		RPCThreadingMode = root["net"]["rpc_threading"].As<std::string>(RPCThreadingMode);
		HttpThreadingMode = root["net"]["http_threading"].As<std::string>(HttpThreadingMode);
		WebRoot = root["net"]["webroot"].As<std::string>(WebRoot);
		MaxUpdatesSize = root["net"]["maxupdatessize"].As<size_t>(MaxUpdatesSize);
		EnableSecurity = root["net"]["security"].As<bool>(EnableSecurity);
		EnableGRPC = root["net"]["grpc"].As<bool>(EnableGRPC);
		GRPCAddr = root["net"]["grpcaddr"].As<std::string>(GRPCAddr);
		TxIdleTimeout = std::chrono::seconds(root["net"]["tx_idle_timeout"].As<int>(TxIdleTimeout.count()));
		HttpReadTimeout = std::chrono::seconds(root["net"]["http_read_timeout"].As<int>(HttpReadTimeout.count()));
		const auto httpWriteTimeout = root["net"]["http_write_timeout"].As<int>(-1);
		if (httpWriteTimeout >= 0) {
			SetHttpWriteTimeout(std::chrono::seconds(httpWriteTimeout));
		}
		MaxHttpReqSize = root["net"]["max_http_body_size"].As<std::size_t>(MaxHttpReqSize);
		EnablePrometheus = root["metrics"]["prometheus"].As<bool>(EnablePrometheus);
		PrometheusCollectPeriod = std::chrono::milliseconds(root["metrics"]["collect_period"].As<int>(PrometheusCollectPeriod.count()));
		EnableConnectionsStats = root["metrics"]["clientsstats"].As<bool>(EnableConnectionsStats);
#ifndef _WIN32
		UserName = root["system"]["user"].As<std::string>(UserName);
		Daemonize = root["system"]["daemonize"].As<bool>(Daemonize);
		DaemonPidFile = root["system"]["pidfile"].As<std::string>(DaemonPidFile);
#endif
		DebugAllocs = root["debug"]["allocs"].As<bool>(DebugAllocs);
		DebugPprof = root["debug"]["pprof"].As<bool>(DebugPprof);
	} catch (const Yaml::Exception &ex) {
		return Error(errParams, "%s", ex.Message());
	}
	return Error();
}

}  // namespace reindexer_server
