#pragma once

#include <functional>
#include <string>
#include <unordered_map>
#include <vector>
#include "cluster/config.h"
#include "estl/fast_hash_set.h"
#include "estl/mutex.h"
#include "estl/shared_mutex.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {
class JsonBuilder;
class RdxContext;
class WrSerializer;

enum ConfigType { ProfilingConf, NamespaceDataConf, AsyncReplicationConf, ReplicationConf };

struct LongQueriesLoggingParams {
	int32_t thresholdUs = -1;
	bool normalized = false;
};

struct LongTxLoggingParams {
	LongTxLoggingParams() noexcept : thresholdUs(-1), avgTxStepThresholdUs(-1) {}

	// Do not using 2 int32's here due to MSVC compatibility reasons (alignof should not be less than sizeof in this case to use it in
	// atomic).
	// Starting from C++14 both of the bit fields will be signed.
	int64_t thresholdUs : 32;
	int64_t avgTxStepThresholdUs : 32;
};

struct ProfilingConfigData {
	bool queriesPerfStats = false;
	size_t queriedThresholdUS = 10;
	bool perfStats = false;
	bool memStats = false;
	bool activityStats = false;
	LongQueriesLoggingParams longSelectLoggingParams;
	LongQueriesLoggingParams longUpdDelLoggingParams;
	LongTxLoggingParams longTxLoggingParams;
};

struct NamespaceConfigData {
	bool lazyLoad = false;
	int noQueryIdleThreshold = 0;
	LogLevel logLevel = LogNone;
	CacheMode cacheMode = CacheModeOff;
	StrictMode strictMode = StrictModeNames;
	int startCopyPolicyTxSize = 10000;
	int copyPolicyMultiplier = 5;
	int txSizeToAlwaysCopy = 100000;
	int optimizationTimeout = 800;
	int optimizationSortWorkers = 4;
	int64_t walSize = 4000000;
	int64_t minPreselectSize = 1000;
	int64_t maxPreselectSize = 1000;
	double maxPreselectPart = 0.1;
	bool idxUpdatesCountingMode = false;
	int syncStorageFlushLimit = 25000;
};

struct ReplicationConfigData {
	int serverID = 0;
	int clusterID = 1;

	Error FromYAML(const std::string &yml);
	Error FromJSON(std::string_view json);
	Error FromJSON(const gason::JsonNode &v);
	void GetJSON(JsonBuilder &jb) const;
	void GetYAML(WrSerializer &ser) const;

	bool operator==(const ReplicationConfigData &rdata) const noexcept {
		return (clusterID == rdata.clusterID) && (serverID == rdata.serverID);
	}
	bool operator!=(const ReplicationConfigData &rdata) const noexcept { return !operator==(rdata); }
};

class DBConfigProvider {
public:
	DBConfigProvider() = default;
	~DBConfigProvider() = default;
	DBConfigProvider(DBConfigProvider &obj) = delete;
	DBConfigProvider &operator=(DBConfigProvider &obj) = delete;

	Error FromJSON(const gason::JsonNode &root);
	void setHandler(ConfigType cfgType, std::function<void()> handler);
	int setHandler(std::function<void(ReplicationConfigData)> handler);
	void unsetHandler(int id);

	ProfilingConfigData GetProfilingConfig();
	cluster::AsyncReplConfigData GetAsyncReplicationConfig();
	ReplicationConfigData GetReplicationConfig();
	bool GetNamespaceConfig(const std::string &nsName, NamespaceConfigData &data);
	LongQueriesLoggingParams GetSelectLoggingParams();
	LongQueriesLoggingParams GetUpdDelLoggingParams();
	LongTxLoggingParams GetTxLoggingParams();
	bool ActivityStatsEnabled();

private:
	ProfilingConfigData profilingData_;
	cluster::AsyncReplConfigData asyncReplicationData_;
	ReplicationConfigData replicationData_;
	std::unordered_map<std::string, NamespaceConfigData> namespacesData_;
	std::unordered_map<int, std::function<void()>> handlers_;
	std::unordered_map<int, std::function<void(ReplicationConfigData)>> replicationConfigDataHandlers_;
	int HandlersCounter_ = 0;
	shared_timed_mutex mtx_;
};

}  // namespace reindexer
