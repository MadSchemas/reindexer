#pragma once

#include <string_view>
#include "cluster/config.h"
#include "estl/contexted_cond_var.h"
#include "estl/fast_hash_set.h"
#include "estl/mutex.h"
#include "estl/shared_mutex.h"
#include "tools/stringstools.h"

// #include "vendor/spdlog/fmt/fmt.h"

namespace reindexer {
namespace cluster {

static constexpr size_t k16kCoroStack = 16 * 1024;

template <typename MtxT = shared_timed_mutex>
class SharedSyncState {
public:
	using GetNameF = std::function<std::string()>;
	using ContainerT = fast_hash_set<std::string, nocase_hash_str, nocase_equal_str, nocase_less_str>;

	void MarkSynchronized(std::string name) {
		// const std::string n = std::move(name);
		std::unique_lock<MtxT> lck(mtx_);
		if (current_.role == RaftInfo::Role::Leader) {
			auto res = synchronized_.emplace(std::move(name));
			lck.unlock();
			if (res.second) {
				// std::cout << fmt::sprintf("Marking '%s' as synchronized (with notification)\n", n);
				cond_.notify_all();
			}
			// else {
			//	std::cout << fmt::sprintf("Marking '%s' as synchronized (no notification)\n", n);
			// }
		}
		// else {
		//	std::cout << fmt::sprintf("Attempt to mark '%s' as synchronized, but current role is %s\n", n,
		//							  RaftInfo::RoleToStr(current_.role));
		// }
	}
	void MarkSynchronized() {
		std::unique_lock<MtxT> lck(mtx_);
		if (current_.role == RaftInfo::Role::Leader) {
			++initialSyncDoneCnt_;

			// std::cout << fmt::sprintf("Marking 'whole DB' as synchronized (with notification); initialSyncDoneCnt_ = %d\n",
			//						  initialSyncDoneCnt_);
			lck.unlock();
			cond_.notify_all();
		}
		// else {
		//	std::cout << fmt::sprintf("Attempt to mark 'whole DB' as synchronized, but current role is %s\n",
		//							  RaftInfo::RoleToStr(current_.role));
		// }
	}
	void Reset(ContainerT requireSynchronization, size_t ReplThreadsCnt, bool enabled) {
		std::lock_guard<MtxT> lck(mtx_);
		requireSynchronization_ = std::move(requireSynchronization);
		synchronized_.clear();
		enabled_ = enabled;
		terminated_ = false;
		initialSyncDoneCnt_ = 0;
		ReplThreadsCnt_ = ReplThreadsCnt;
		next_ = current_ = RaftInfo();
		assert(ReplThreadsCnt_);
		// std::cout << fmt::sprintf("Reseting sync state\n");
	}
	template <typename ContextT>
	void AwaitInitialSync(std::string_view name, const ContextT& ctx) const {
		nocase_hash_str h;
		std::size_t hash = h(name);
		shared_lock<MtxT> lck(mtx_);
		while (!isInitialSyncDone(name, hash)) {
			if (terminated_) {
				throw Error(errTerminated, "Cluster was terminated");
			}
			if (next_.role == RaftInfo::Role::Follower) {
				throw Error(errWrongReplicationData, "Node role was changed to follower");
			}
			// std::cout << fmt::sprintf("Initial sync is not done for '%s', TID: %s, hash: %d; Awaiting...\n", name,
			//						  std::this_thread::get_id(), hash);
			// try {
			cond_.wait(
				lck,
				[this, &name, hash]() noexcept {
					auto res = isInitialSyncDone(name, hash) || terminated_ || next_.role == RaftInfo::Role::Follower;
					// nocase_hash_str h;
					// std::cout << fmt::sprintf("AwaitInitialSync(%s / %d / %d) lambda call: %d, terminated_: %d, next_.role: %d\n", name,
					//						  hash, h(name), int(res), int(terminated_), int(next_.role));
					return res;
				},
				ctx);
			//} catch (...) {
			//	std::cout << fmt::sprintf("!!!Exception in AwaitInitialSync(%s)\n", name);
			//	throw;
			//}
			// std::cout << fmt::sprintf("Initial sync is done for '%s', TID: %s, hash: %d!\n", name, std::this_thread::get_id(), hash);
		}
	}
	template <typename ContextT>
	void AwaitInitialSync(const ContextT& ctx) const {
		shared_lock<MtxT> lck(mtx_);
		while (!isInitialSyncDone()) {
			if (terminated_) {
				throw Error(errTerminated, "Cluster was terminated");
			}
			if (next_.role == RaftInfo::Role::Follower) {
				throw Error(errWrongReplicationData, "Node role was changed to follower");
			}
			// std::cout << fmt::sprintf("Initial sync is not done for 'whole DB', awaiting...\n");
			// try {
			cond_.wait(
				lck,
				[this]() noexcept {
					auto res = isInitialSyncDone() || terminated_ || next_.role == RaftInfo::Role::Follower;
					// std::cout << fmt::sprintf("AwaitInitialSync() lambda call: %d, terminated_: %d, next_.role: %d\n", int(res),
					//						  int(terminated_), int(next_.role));
					return res;
				},
				ctx);
			//} catch (...) {
			//	std::cout << "!!!Exception in AwaitInitialSync()\n";
			//	throw;
			//}
			// std::cout << fmt::sprintf("Initial sync is done for 'whole DB'!\n");
		}
	}
	bool IsInitialSyncDone(std::string_view name) const {
		nocase_hash_str h;
		std::size_t hash = h(name);
		shared_lock<MtxT> lck(mtx_);
		return isInitialSyncDone(name, hash);
	}
	bool IsInitialSyncDone() const {
		shared_lock<MtxT> lck(mtx_);
		return isInitialSyncDone();
	}
	RaftInfo TryTransitRole(RaftInfo expected) {
		std::unique_lock<MtxT> lck(mtx_);
		if (expected == next_) {
			if (current_.role == RaftInfo::Role::Leader && current_.role != next_.role) {
				// std::cout << fmt::sprintf("Clearing synchronized list on role switch\n");
				synchronized_.clear();
				initialSyncDoneCnt_ = 0;
			}
			current_ = next_;
			// std::cout << fmt::sprintf("Role transition done, sending notification!\n");
			lck.unlock();
			cond_.notify_all();
			return expected;
		}
		return next_;
	}
	template <typename ContextT>
	RaftInfo AwaitRole(bool allowTransitState, const ContextT& ctx) const {
		shared_lock<MtxT> lck(mtx_);
		if (allowTransitState) {
			// try {
			cond_.wait(
				lck, [this] { return !isRunning() || next_ == current_; }, ctx);
			//} catch (...) {
			//	std::cout << fmt::sprintf("!!!Exception in AwaitRole(allowTransitState=true)\n");
			//	throw;
			//}
		} else {
			// std::cout << fmt::sprintf("Awaiting role transition... Current role is %s\n", RaftInfo::RoleToStr(current_.role));
			// try {
			cond_.wait(
				lck,
				[this] {
					return !isRunning() ||
						   (next_ == current_ && (current_.role == RaftInfo::Role::Leader || current_.role == RaftInfo::Role::Follower));
				},
				ctx);
			//} catch (...) {
			//	std::cout << fmt::sprintf("!!!Exception in AwaitRole(allowTransitState=false)\n");
			//	throw;
			//}
			// std::cout << fmt::sprintf("Role transition done! Current role is %s\n", RaftInfo::RoleToStr(current_.role));
		}
		return current_;
	}
	void SetRole(RaftInfo info) {
		std::lock_guard<MtxT> lck(mtx_);
		next_ = info;
	}
	std::pair<RaftInfo, RaftInfo> GetRolesPair() const {
		shared_lock<MtxT> lck(mtx_);
		return std::make_pair(current_, next_);
	}
	RaftInfo CurrentRole() const {
		shared_lock<MtxT> lck(mtx_);
		return current_;
	}
	void SetTerminated() {
		{
			std::lock_guard<MtxT> lck(mtx_);
			terminated_ = true;
			next_ = current_ = RaftInfo();
		}
		cond_.notify_all();
	}

private:
	bool isInitialSyncDone(std::string_view name, std::size_t hash) const {
		return !isRequireSync(name, hash) || (current_.role == RaftInfo::Role::Leader && synchronized_.count(name, hash));
	}
	bool isInitialSyncDone() const noexcept {
		return !enabled_ || (next_.role == RaftInfo::Role::Leader && initialSyncDoneCnt_ == ReplThreadsCnt_);
	}
	bool isRequireSync(std::string_view name, size_t hash) const noexcept {
		return enabled_ && (requireSynchronization_.empty() || requireSynchronization_.count(name, hash));
	}
	bool isRunning() const noexcept { return enabled_ && !terminated_; }

	mutable MtxT mtx_;
	mutable contexted_cond_var cond_;
	ContainerT synchronized_;
	ContainerT requireSynchronization_;
	bool enabled_ = false;
	RaftInfo current_;
	RaftInfo next_;
	bool terminated_ = false;
	size_t initialSyncDoneCnt_ = 0;
	size_t ReplThreadsCnt_ = 0;
};
}  // namespace cluster
}  // namespace reindexer
