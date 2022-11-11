#include "rdxcontext.h"
#include "core/queryresults/queryresults.h"

namespace reindexer {

RdxContext::RdxContext(RdxContext&& other)
	: activityPtr_(nullptr),
	  cancelCtx_(other.cancelCtx_),
	  cmpl_(std::move(other.cmpl_)),
	  originLsn_(other.originLsn_),
	  holdStatus_(other.holdStatus_),
	  noWaitSync_(other.noWaitSync_),
	  shardingParallelExecution_{other.shardingParallelExecution_} {
	if (holdStatus_ == HoldT::kHold) {
		new (&activityCtx_) RdxActivityContext(std::move(other.activityCtx_));
	} else if (holdStatus_ == HoldT::kPtr) {
		activityPtr_ = other.activityPtr_;
	}
}

RdxContext::~RdxContext() {
	if (holdStatus_ == HoldT::kHold) {
		activityCtx_.~RdxActivityContext();
	} else if (holdStatus_ == HoldT::kPtr) {
		[[maybe_unused]] const auto refs = activityPtr_->refCount_.fetch_sub(1, std::memory_order_relaxed);
		assertrx(refs != 0u);
	}
}

RdxActivityContext* RdxContext::Activity() const noexcept {
	switch (holdStatus_) {
		case HoldT::kHold:
			return &activityCtx_;
		case HoldT::kPtr:
			return activityPtr_;
		default:
			return nullptr;
	}
}

RdxActivityContext::Ward RdxContext::BeforeLock(MutexMark mutexMark) const {
	switch (holdStatus_) {
		case HoldT::kHold:
			return activityCtx_.BeforeLock(mutexMark);
		case HoldT::kPtr:
			return activityPtr_->BeforeLock(mutexMark);
		default:
			return RdxActivityContext::Ward{nullptr, mutexMark};
	}
}

RdxActivityContext::Ward RdxContext::BeforeIndexWork() const {
	switch (holdStatus_) {
		case HoldT::kHold:
			return activityCtx_.BeforeIndexWork();
		case HoldT::kPtr:
			return activityPtr_->BeforeIndexWork();
		default:
			return RdxActivityContext::Ward{nullptr, Activity::IndexesLookup};
	}
}

RdxActivityContext::Ward RdxContext::BeforeSelectLoop() const {
	switch (holdStatus_) {
		case HoldT::kHold:
			return activityCtx_.BeforeSelectLoop();
		case HoldT::kPtr:
			return activityPtr_->BeforeSelectLoop();
		default:
			return RdxActivityContext::Ward{nullptr, Activity::SelectLoop};
	}
}

RdxActivityContext::Ward RdxContext::BeforeClusterProxy() const {
	switch (holdStatus_) {
		case HoldT::kHold:
			return activityCtx_.BeforeClusterProxy();
		case HoldT::kPtr:
			return activityPtr_->BeforeClusterProxy();
		default:
			return RdxActivityContext::Ward{nullptr, Activity::ProxiedViaClusterProxy};
	}
}
RdxActivityContext::Ward RdxContext::BeforeShardingProxy() const {
	switch (holdStatus_) {
		case HoldT::kHold:
			return activityCtx_.BeforeShardingProxy();
		case HoldT::kPtr:
			return activityPtr_->BeforeShardingProxy();
		default:
			return RdxActivityContext::Ward{nullptr, Activity::ProxiedViaShardingProxy};
	}
}

RdxActivityContext::Ward RdxContext::BeforeSimpleState(Activity::State st) const {
	assert(st != Activity::WaitLock);
	switch (holdStatus_) {
		case HoldT::kHold:
			return activityCtx_.BeforeState(st);
		case HoldT::kPtr:
			return activityPtr_->BeforeState(st);
		default:
			return RdxActivityContext::Ward{nullptr, Activity::IndexesLookup};
	}
}

RdxContext InternalRdxContext::CreateRdxContext(std::string_view query, ActivityContainer& activityContainer) const {
	if (activityTracer_.empty() || query.empty()) {
		return {LSN(), (deadlineCtx_.IsCancelable() ? &deadlineCtx_ : nullptr), cmpl_, emmiterServerId_, shardingParallelExecution_};
	} else {
		return {LSN(),
				activityTracer_,
				user_,
				query,
				activityContainer,
				connectionId_,
				(deadlineCtx_.IsCancelable() ? &deadlineCtx_ : nullptr),
				cmpl_,
				emmiterServerId_,
				shardingParallelExecution_};
	}
}

RdxContext InternalRdxContext::CreateRdxContext(std::string_view query, ActivityContainer& activityContainer,
												QueryResults& qresults) const {
	if (activityTracer_.empty() || query.empty())
		return {LSN(), (deadlineCtx_.IsCancelable() ? &deadlineCtx_ : nullptr), cmpl_, emmiterServerId_, shardingParallelExecution_};
	assertrx(!qresults.activityCtx_);
	qresults.activityCtx_.emplace(activityTracer_, user_, query, activityContainer, connectionId_, true);
	return RdxContext{&*(qresults.activityCtx_), LSN(), (deadlineCtx_.IsCancelable() ? &deadlineCtx_ : nullptr), cmpl_, emmiterServerId_,
					  shardingParallelExecution_};
}

}  // namespace reindexer
