#include "joinedselector.h"

#include "core/namespace/namespaceimpl.h"
#include "core/queryresults/joinresults.h"
#include "estl/restricted.h"
#include "nsselecter.h"
#include "vendor/sparse-map/sparse_set.h"

constexpr size_t kMaxIterationsScaleForInnerJoinOptimization = 100;

namespace reindexer {

void JoinedSelector::selectFromRightNs(QueryResults &joinItemR, const Query &query, bool &found, bool &matchedAtLeastOnce) {
	assertrx(rightNs_);

	JoinCacheRes joinResLong;
	rightNs_->getFromJoinCache(query, joinQuery_, joinResLong);

	rightNs_->getIndsideFromJoinCache(joinRes_);
	if (joinRes_.needPut) {
		rightNs_->putToJoinCache(joinRes_, preResult_);
	}
	if (joinResLong.haveData) {
		found = joinResLong.it.val.ids_->size();
		matchedAtLeastOnce = joinResLong.it.val.matchedAtLeastOnce;
		rightNs_->FillResult(joinItemR, *joinResLong.it.val.ids_);
	} else {
		SelectCtx ctx(query, nullptr);
		ctx.preResult = preResult_;
		ctx.matchedAtLeastOnce = false;
		ctx.reqMatchedOnceFlag = true;
		ctx.skipIndexesLookup = true;
		ctx.functions = &selectFunctions_;
		rightNs_->Select(joinItemR, ctx, rdxCtx_);
		if (query.GetExplain()) {
			preResult_->explainOneSelect = joinItemR.explainResults;
		}

		found = joinItemR.Count();
		matchedAtLeastOnce = ctx.matchedAtLeastOnce;
	}
	if (joinResLong.needPut) {
		JoinCacheVal val;
		val.ids_ = make_intrusive<intrusive_atomic_rc_wrapper<IdSet>>();
		val.matchedAtLeastOnce = matchedAtLeastOnce;
		for (auto &r : joinItemR.Items()) {
			val.ids_->Add(r.Id(), IdSet::Unordered, 0);
		}
		rightNs_->putToJoinCache(joinResLong, std::move(val));
	}
}

void JoinedSelector::selectFromPreResultValues(QueryResults &joinItemR, const Query &query, bool &found, bool &matchedAtLeastOnce) const {
	size_t matched = 0;
	const JoinPreResult::Values &values = std::get<JoinPreResult::Values>(preResult_->preselectedPayload);
	for (const ItemRef &item : values) {
		auto &v = item.Value();
		assertrx(!v.IsFree());
		if (query.Entries().CheckIfSatisfyConditions({values.payloadType, v})) {
			if (++matched > query.Limit()) break;
			found = true;
			joinItemR.Add(item);
		}
	}
	matchedAtLeastOnce = matched;
}

bool JoinedSelector::Process(IdType rowId, int nsId, ConstPayload payload, bool match) {
	++called_;
	if (optimized_ && !match) {
		matched_++;
		return true;
	}

	const auto startTime = ExplainCalc::Clock::now();
	// Put values to join conditions
	size_t i = 0;
	if (itemQuery_.GetExplain() && !preResult_->explainOneSelect.empty()) itemQuery_.Explain(false);
	std::unique_ptr<Query> itemQueryCopy;
	Query *itemQueryPtr = &itemQuery_;
	for (auto &je : joinQuery_.joinEntries_) {
		QueryEntry &qentry = itemQueryPtr->GetUpdatableEntry<QueryEntry>(i);
		{
			auto keyValues = qentry.UpdatableValues(QueryEntry::IgnoreEmptyValues{});
			payload.GetByFieldsSet(je.LeftFields(), keyValues, je.LeftFieldType(), je.LeftCompositeFieldsTypes());
		}
		if (qentry.Values().empty()) {
			if (itemQueryPtr == &itemQuery_) {
				itemQueryCopy = std::make_unique<Query>(itemQuery_);
				itemQueryPtr = itemQueryCopy.get();
			}
			itemQueryPtr->SetEntry<AlwaysFalse>(i);
		}
		++i;
	}
	itemQueryPtr->Limit(match ? joinQuery_.Limit() : 0);

	bool found = false;
	bool matchedAtLeastOnce = false;
	QueryResults joinItemR;
	std::visit(
		overloaded{[&](const JoinPreResult::Values &) { selectFromPreResultValues(joinItemR, *itemQueryPtr, found, matchedAtLeastOnce); },
				   Restricted<IdSet, SelectIteratorContainer>{}(
					   [&](const auto &) { selectFromRightNs(joinItemR, *itemQueryPtr, found, matchedAtLeastOnce); })},
		preResult_->preselectedPayload);
	if (match && found) {
		if (nsId >= static_cast<int>(result_.joined_.size())) {
			result_.joined_.resize(nsId + 1);
		}
		joins::NamespaceResults &nsJoinRes = result_.joined_[nsId];
		nsJoinRes.SetJoinedSelectorsCount(joinedSelectorsCount_);
		nsJoinRes.Insert(rowId, joinedFieldIdx_, std::move(joinItemR));
	}
	if (matchedAtLeastOnce) ++matched_;
	preResult_->selectTime += (ExplainCalc::Clock::now() - startTime);
	return matchedAtLeastOnce;
}

template <typename Cont, typename Fn>
VariantArray JoinedSelector::readValuesOfRightNsFrom(const Cont &data, const Fn &createPayload, const QueryJoinEntry &entry,
													 const PayloadType &pt) const {
	const auto rightFieldType = entry.RightFieldType();
	const auto leftFieldType = entry.LeftFieldType();
	VariantArray res;
	if (rightFieldType.Is<KeyValueType::Composite>()) {
		unordered_payload_set set(data.size(), hash_composite(pt, entry.RightFields()), equal_composite(pt, entry.RightFields()));
		for (const auto &v : data) {
			const auto pl = createPayload(v);
			if (pl) {
				set.insert(*pl->Value());
			}
		}
		res.reserve(set.size());
		for (auto &s : set) {
			res.emplace_back(std::move(s));
		}
	} else {
		tsl::sparse_set<Variant> set(data.size());
		for (const auto &v : data) {
			const auto pl = createPayload(v);
			if (!pl) {
				continue;
			}
			pl->GetByFieldsSet(entry.RightFields(), res, entry.RightFieldType(), entry.RightCompositeFieldsTypes());
			if (!leftFieldType.Is<KeyValueType::Undefined>() && !leftFieldType.Is<KeyValueType::Composite>()) {
				for (Variant &v : res) set.insert(std::move(v.convert(leftFieldType)));
			} else {
				for (Variant &v : res) set.insert(std::move(v));
			}
		}
		res.clear<false>();
		for (auto &s : set) {
			res.emplace_back(std::move(s));
		}
	}
	return res;
}

VariantArray JoinedSelector::readValuesFromRightNs(const QueryJoinEntry &entry) const {
	return readValuesOfRightNsFrom(
		std::get<IdSet>(preResult_->preselectedPayload),
		[this](IdType rowId) -> std::optional<ConstPayload> {
			const auto &item = rightNs_->items_[rowId];
			if (item.IsFree()) {
				return std::nullopt;
			}
			return ConstPayload{rightNs_->payloadType_, item};
		},
		entry, rightNs_->payloadType_);
}

VariantArray JoinedSelector::readValuesFromPreResult(const QueryJoinEntry &entry) const {
	const JoinPreResult::Values &values = std::get<JoinPreResult::Values>(preResult_->preselectedPayload);
	return readValuesOfRightNsFrom(
		values,
		[&values](const ItemRef &item) -> std::optional<ConstPayload> {
			if (item.Value().IsFree()) {
				return std::nullopt;
			}
			return ConstPayload{values.payloadType, item.Value()};
		},
		entry, values.payloadType);
}

void JoinedSelector::AppendSelectIteratorOfJoinIndexData(SelectIteratorContainer &iterators, int *maxIterations, unsigned sortId,
														 const SelectFunction::Ptr &selectFnc, const RdxContext &rdxCtx) {
	if (joinType_ != JoinType::InnerJoin || preResult_->executionMode != JoinPreResult::ModeExecute ||
		std::visit(overloaded{[](const SelectIteratorContainer &) noexcept { return true; },
							  Restricted<IdSet, JoinPreResult::Values>{}([maxIterations](const auto &v) noexcept {
								  return v.size() > *maxIterations * kMaxIterationsScaleForInnerJoinOptimization;
							  })},
				   preResult_->preselectedPayload)) {
		return;
	}
	unsigned optimized = 0;
	assertrx_throw(!std::holds_alternative<JoinPreResult::Values>(preResult_->preselectedPayload) ||
				   itemQuery_.Entries().Size() == joinQuery_.joinEntries_.size());
	for (size_t i = 0; i < joinQuery_.joinEntries_.size(); ++i) {
		const QueryJoinEntry &joinEntry = joinQuery_.joinEntries_[i];
		if (!joinEntry.IsLeftFieldIndexed() || joinEntry.Operation() != OpAnd ||
			(joinEntry.Condition() != CondEq && joinEntry.Condition() != CondSet) ||
			(i + 1 < joinQuery_.joinEntries_.size() && joinQuery_.joinEntries_[i + 1].Operation() == OpOr)) {
			continue;
		}
		const auto &leftIndex = leftNs_->indexes_[joinEntry.LeftIdxNo()];
		assertrx(!IsFullText(leftIndex->Type()));
		if (leftIndex->Opts().IsSparse()) continue;

		const VariantArray values = std::visit(overloaded{[&](const IdSet &) { return readValuesFromRightNs(joinEntry); },
														  [&](const JoinPreResult::Values &) { return readValuesFromPreResult(joinEntry); },
														  [](const SelectIteratorContainer &) -> VariantArray {
															  assertrx_throw(0);
															  abort();
														  }},
											   preResult_->preselectedPayload);
		auto ctx = selectFnc ? selectFnc->CreateCtx(joinEntry.LeftIdxNo()) : BaseFunctionCtx::Ptr{};
		assertrx(!ctx || ctx->type != BaseFunctionCtx::kFtCtx);

		if (leftIndex->Opts().GetCollateMode() == CollateUTF8) {
			for (auto &key : values) key.EnsureUTF8();
		}
		Index::SelectOpts opts;
		opts.maxIterations = iterators.GetMaxIterations();
		opts.indexesNotOptimized = !leftNs_->SortOrdersBuilt();
		opts.inTransaction = inTransaction_;

		bool was = false;
		for (SelectKeyResult &res : leftIndex->SelectKey(values, CondSet, sortId, opts, ctx, rdxCtx)) {
			if (!res.comparators_.empty()) continue;
			SelectIterator selIter{res, false, joinEntry.LeftFieldName(),
								   (joinEntry.LeftIdxNo() < 0 ? IteratorFieldKind::NonIndexed : IteratorFieldKind::Indexed), false};
			selIter.Bind(leftNs_->payloadType_, joinEntry.LeftIdxNo());
			const int curIterations = selIter.GetMaxIterations();
			if (curIterations && curIterations < *maxIterations) *maxIterations = curIterations;
			iterators.Append(OpAnd, std::move(selIter));
			was = true;
		}
		if (was) ++optimized;
	}
	optimized_ = optimized == joinQuery_.joinEntries_.size();
}

}  // namespace reindexer
