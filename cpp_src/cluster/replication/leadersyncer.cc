#include "leadersyncer.h"
#include "client/snapshot.h"
#include "cluster/logger.h"
#include "core/reindexerimpl.h"

namespace reindexer {
namespace cluster {

Error LeaderSyncer::Sync(std::list<LeaderSyncQueue::Entry>&& entries, SharedSyncState<>& sharedSyncState, ReindexerImpl& thisNode,
						 ReplicationStatsCollector statsCollector) {
	Error err;
	const LeaderSyncThread::Config thCfg{cfg_.dsns,		cfg_.maxWALDepthOnForceSync, cfg_.clusterId,
										 cfg_.serverId, cfg_.enableCompression,		 cfg_.netTimeout};
	std::unique_lock lck(mtx_);
	syncQueue_.Refill(std::move(entries));
	assert(threads_.empty());
	for (size_t i = 0; i < cfg_.threadsCount; ++i) {
		threads_.emplace_back(thCfg, syncQueue_, sharedSyncState, thisNode, statsCollector, log_);
	}
	lck.unlock();

	for (auto& th : threads_) {
		th.Join();
		if (err.ok()) {
			err = th.LastError();
		}
	}

	bool wasTerminated = false;
	for (auto& th : threads_) {
		if (th.IsTerminated()) {
			wasTerminated = true;
			break;
		}
	}
	if (!wasTerminated) {
		assert(syncQueue_.Size() == 0);
	}

	lck.lock();
	threads_.clear();
	return err;
}

void LeaderSyncThread::sync() {
	loop_.spawn([this] {
		LeaderSyncQueue::Entry entry;
		uint32_t nodeId = 0;
		int32_t preferredNodeId = -1;
		uint64_t expectedDataHash = 0;
		while (syncQueue_.TryToGetEntry(preferredNodeId, entry, nodeId, expectedDataHash)) {
			if (preferredNodeId != int32_t(nodeId)) {
				preferredNodeId = int32_t(nodeId);
				client_.Stop();
			}
			logInfo("%d: Trying to sync ns '%s' from %d (TID: %d)", cfg_.serverId, entry.nsName, nodeId, std::this_thread::get_id());
			std::string tmpNsName;
			try {
				// 2) Recv most recent data
				auto err = client_.Connect(cfg_.dsns[nodeId], loop_, client::ConnectOpts().WithExpectedClusterID(cfg_.clusterId));
				if (!err.ok()) {
					throw err;
				}
				for (int retry = 0; retry < 2; ++retry) {
					const bool fullResync = retry > 0;
					syncNamespaceImpl(fullResync, entry, tmpNsName);
					ReplicationStateV2 state;
					err = thisNode_.GetReplState(entry.nsName, state, RdxContext());
					if (!err.ok()) {
						throw err;
					}
					const auto localLsn = ExtendedLsn(state.nsVersion, state.lastLsn);
					if (state.dataHash == expectedDataHash) {
						logInfo("%d: Local namespace '%s' was updated from node %d (ns version: %d, lsn: %d)", cfg_.serverId, entry.nsName,
								nodeId, localLsn.NsVersion(), localLsn.LSN());
						break;
					}

					if (fullResync) {
						throw Error(errDataHashMismatch,
									"%d: Datahash missmatch after full resync for local namespce '%s'. Expected: %d; actual: %d",
									cfg_.serverId, entry.nsName, expectedDataHash, state.dataHash);
					}
					logWarn(
						"%d: Datahash missmatch after local namespace '%s' sync. Expected: %d, actual: "
						"%d. Forcing "
						"full resync...",
						cfg_.serverId, entry.nsName, expectedDataHash, state.dataHash);
				}
				sharedSyncState_.MarkSynchronized(std::string(entry.nsName));
			} catch (const Error& err) {
				lastError_ = err;
				logError("%d: Unable to sync local namespace '%s': %s", cfg_.serverId, entry.nsName, lastError_.what());
				if (!tmpNsName.empty()) {
					logError("%d: Dropping '%s'...", cfg_.serverId, tmpNsName);
					thisNode_.DropNamespace(tmpNsName, RdxContext());
					logError("%d: '%s' was dropped", cfg_.serverId, tmpNsName);
				}
			} catch (...) {
				lastError_ = Error(errLogic, "Unexpected exception");
				logError("%d: Unable to sync local namespace '%s': %s", cfg_.serverId, entry.nsName, lastError_.what());
				if (!tmpNsName.empty()) {
					logError("%d: Dropping '%s'...", cfg_.serverId, tmpNsName);
					thisNode_.DropNamespace(tmpNsName, RdxContext());
					logError("%d: '%s' was dropped", cfg_.serverId, tmpNsName);
				}
			}
			syncQueue_.SyncDone(nodeId);
			client_.Stop();
		}
	});
	loop_.run();
}

void LeaderSyncThread::syncNamespaceImpl(bool forced, const LeaderSyncQueue::Entry& syncEntry, std::string& tmpNsName) {
	logInfo("%d: '%s'. Trying to synchronize namespace %s", cfg_.serverId, syncEntry.nsName, forced ? "forced" : "by wal");
	SyncTimeCounter timeCounter(SyncTimeCounter::Type::InitialWalSync, statsCollector_);
	client::Snapshot snapshot;
	auto err = client_.GetSnapshot(syncEntry.nsName, SnapshotOpts(forced ? ExtendedLsn() : syncEntry.localLsn, cfg_.maxWALDepthOnForceSync),
								   snapshot);
	if (!err.ok()) {
		throw err;
	}

	RdxContext ctx;
	ctx.WithNoWaitSync();
	auto ns = thisNode_.getNamespaceNoThrow(syncEntry.nsName, ctx);
	if (!ns || snapshot.HasRawData()) {
		timeCounter.SetType(SyncTimeCounter::Type::InitialForceSync);
		// TODO: Allow tmp ns without storage via config
		err = thisNode_.CreateTemporaryNamespace(syncEntry.nsName, tmpNsName, StorageOpts().Enabled(), syncEntry.latestLsn.NsVersion(),
												 RdxContext());
		if (!err.ok()) {
			throw err;
		}
		ns = thisNode_.getNamespaceNoThrow(tmpNsName, ctx);
		assert(ns);
	}

	for (auto& ch : snapshot) {
		if (terminate_) {
			return;
		}
		ns->ApplySnapshotChunk(ch.Chunk(), true, ctx);
	}

	if (!tmpNsName.empty()) {
		err = thisNode_.renameNamespace(tmpNsName, std::string(syncEntry.nsName), true, true);
		if (!err.ok()) {
			throw err;
		}
	}
}

}  // namespace cluster
}  // namespace reindexer
