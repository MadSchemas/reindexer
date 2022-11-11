#include "selecter.h"
#include "core/ft/bm25.h"
#include "core/ft/typos.h"
#include "core/rdxcontext.h"
#include "sort/pdqsort.hpp"
#include "tools/logger.h"

namespace {
static double pos2rank(int pos) {
	if (pos <= 10) return 1.0 - (pos / 100.0);
	if (pos <= 100) return 0.9 - (pos / 1000.0);
	if (pos <= 1000) return 0.8 - (pos / 10000.0);
	if (pos <= 10000) return 0.7 - (pos / 100000.0);
	if (pos <= 100000) return 0.6 - (pos / 1000000.0);
	return 0.5;
}
}  // namespace

namespace reindexer {
// Relevancy procent of full word match
const int kFullMatchProc = 100;
// Mininum relevancy procent of prefix word match.
const int kPrefixMinProc = 50;
const int kSuffixMinProc = 10;
// Maximum relevancy procent of typo match
const int kTypoProc = 85;
// Relevancy step of typo match
const int kTypoStepProc = 15;
// Decrease procent of relevancy if pattern found by word stem
const int kStemProcDecrease = 15;

template <typename IdCont>
void Selecter<IdCont>::prepareVariants(std::vector<FtVariantEntry> &variants, size_t termIdx, const std::vector<std::string> &langs,
									   const FtDSLQuery &dsl, std::vector<SynonymsDsl> *synonymsDsl) {
	const FtDSLEntry &term = dsl[termIdx];
	variants.clear();

	std::vector<std::pair<std::wstring, int>> variantsUtf16{{term.pattern, kFullMatchProc}};

	if (synonymsDsl && (!holder_.cfg_->enableNumbersSearch || !term.opts.number)) {
		// Make translit and kblayout variants
		if (holder_.cfg_->enableTranslit && !term.opts.exact) {
			holder_.translit_->GetVariants(term.pattern, variantsUtf16);
		}
		if (holder_.cfg_->enableKbLayout && !term.opts.exact) {
			holder_.kbLayout_->GetVariants(term.pattern, variantsUtf16);
		}
		// Synonyms
		if (term.opts.op != OpNot) {
			holder_.synonyms_->GetVariants(term.pattern, variantsUtf16);
			holder_.synonyms_->PostProcess(term, dsl, termIdx, *synonymsDsl);
		}
	}

	// Apply stemmers
	std::string tmpstr, stemstr;
	for (auto &v : variantsUtf16) {
		utf16_to_utf8(v.first, tmpstr);
		if (tmpstr.empty()) continue;
		variants.push_back({tmpstr, term.opts, v.second});
		if (!term.opts.exact) {
			for (auto &lang : langs) {
				auto stemIt = holder_.stemmers_.find(lang);
				if (stemIt == holder_.stemmers_.end()) {
					throw Error(errParams, "Stemmer for language %s is not available", lang);
				}
				stemIt->second.stem(tmpstr, stemstr);
				if (tmpstr != stemstr && !stemstr.empty()) {
					FtDslOpts opts = term.opts;
					opts.pref = true;

					if (&v != &variantsUtf16[0]) opts.suff = false;

					variants.push_back({stemstr, opts, v.second - kStemProcDecrease});
				}
			}
		}
	}
}

template <typename IdCont>
template <bool mergeStatusesEmpty>
IDataHolder::MergeData Selecter<IdCont>::Process(FtDSLQuery &dsl, bool inTransaction, FtMergeStatuses::Statuses mergeStatuses,
												 const RdxContext &rdxCtx) {
	FtSelectContext ctx;
	ctx.rawResults.reserve(dsl.size());
	// STEP 2: Search dsl terms for each variant
	std::vector<SynonymsDsl> synonymsDsl;
	holder_.synonyms_->PreProcess(dsl, synonymsDsl);
	if (!inTransaction) ThrowOnCancel(rdxCtx);
	for (size_t i = 0; i < dsl.size(); ++i) {
		ctx.rawResults.emplace_back();
		TextSearchResults &res = ctx.rawResults.back();
		res.term = dsl[i];

		// Prepare term variants (original + translit + stemmed + kblayout + synonym)
		this->prepareVariants(ctx.variants, i, holder_.cfg_->stemmers, dsl, &synonymsDsl);

		if (holder_.cfg_->logLevel >= LogInfo) {
			WrSerializer wrSer;
			for (auto &variant : ctx.variants) {
				if (&variant != &*ctx.variants.begin()) wrSer << ", ";
				wrSer << variant.pattern;
			}
			wrSer << "], typos: [";
			typos_context tctx[kMaxTyposInWord];
			if (res.term.opts.typos)
				mktypos(tctx, res.term.pattern, holder_.cfg_->MaxTyposInWord(), holder_.cfg_->maxTypoLen,
						[&wrSer](std::string_view typo, int) {
							wrSer << typo;
							wrSer << ", ";
						});
			logPrintf(LogInfo, "Variants: [%s]", wrSer.Slice());
		}

		processVariants<mergeStatusesEmpty>(ctx, mergeStatuses);
		if (res.term.opts.typos) {
			// Lookup typos from typos_ map and fill results
			processTypos(ctx, res.term);
		}
	}

	std::vector<TextSearchResults> results;
	size_t reserveSize = ctx.rawResults.size();
	for (const SynonymsDsl &synDsl : synonymsDsl) reserveSize += synDsl.dsl.size();
	results.reserve(reserveSize);
	std::vector<size_t> synonymsBounds;
	synonymsBounds.reserve(synonymsDsl.size());
	if (!inTransaction) ThrowOnCancel(rdxCtx);
	for (const SynonymsDsl &synDsl : synonymsDsl) {
		FtSelectContext synCtx;
		synCtx.rawResults.reserve(synDsl.dsl.size());
		for (size_t i = 0; i < synDsl.dsl.size(); ++i) {
			synCtx.rawResults.emplace_back();
			synCtx.rawResults.back().term = synDsl.dsl[i];
			prepareVariants(synCtx.variants, i, holder_.cfg_->stemmers, synDsl.dsl, nullptr);
			if (holder_.cfg_->logLevel >= LogInfo) {
				WrSerializer wrSer;
				for (auto &variant : synCtx.variants) {
					if (&variant != &*synCtx.variants.begin()) wrSer << ", ";
					wrSer << variant.pattern;
				}
				logPrintf(LogInfo, "Multiword synonyms variants: [%s]", wrSer.Slice());
			}
			processVariants<mergeStatusesEmpty>(synCtx, mergeStatuses);
		}
		for (size_t idx : synDsl.termsIdx) {
			assertrx(idx < ctx.rawResults.size());
			ctx.rawResults[idx].synonyms.push_back(results.size());
			ctx.rawResults[idx].synonymsGroups.push_back(synonymsBounds.size());
		}
		for (auto &res : synCtx.rawResults) {
			results.emplace_back(std::move(res));
		}
		synonymsBounds.push_back(results.size());
	}

	for (auto &res : ctx.rawResults) results.emplace_back(std::move(res));
	return mergeResults(results, synonymsBounds, inTransaction, std::move(mergeStatuses), rdxCtx);
}

template <typename IdCont>
template <bool mergeStatusesEmpty>
void Selecter<IdCont>::processStepVariants(FtSelectContext &ctx, typename DataHolder<IdCont>::CommitStep &step,
										   const FtVariantEntry &variant, TextSearchResults &res,
										   const FtMergeStatuses::Statuses &mergeStatuses) {
	if (variant.opts.op == OpAnd) {
		ctx.foundWords.clear();
	}
	auto &tmpstr = variant.pattern;
	auto &suffixes = step.suffixes_;
	//  Lookup current variant in suffixes array
	auto keyIt = suffixes.lower_bound(tmpstr);

	int matched = 0, skipped = 0, vids = 0, excludedCnt = 0;
	bool withPrefixes = variant.opts.pref;
	bool withSuffixes = variant.opts.suff;

	// Walk current variant in suffixes array and fill results
	do {
		if (keyIt == suffixes.end()) break;

		WordIdType glbwordId = keyIt->second;

		if constexpr (!mergeStatusesEmpty) {
			bool excluded = true;
			for (const auto &id : holder_.getWordById(glbwordId).vids_) {
				if (mergeStatuses[id.Id()] != FtMergeStatuses::kExcluded) {
					excluded = false;
					break;
				}
			}
			if (excluded) {
				++excludedCnt;
				continue;
			}
		}

		uint32_t suffixWordId = holder_.GetSuffixWordId(glbwordId, step);
		const std::string::value_type *word = suffixes.word_at(suffixWordId);

		int16_t wordLength = suffixes.word_len_at(suffixWordId);

		ptrdiff_t suffixLen = keyIt->first - word;
		const int matchLen = tmpstr.length();

		if (!withSuffixes && suffixLen) continue;
		if (!withPrefixes && wordLength != matchLen + suffixLen) break;

		int matchDif = std::abs(long(wordLength - matchLen + suffixLen));
		int proc = std::max(variant.proc - holder_.cfg_->partialMatchDecrease * matchDif / std::max(matchLen, 3),
							suffixLen ? kSuffixMinProc : kPrefixMinProc);

		auto it = ctx.foundWords.find(glbwordId);
		if (it == ctx.foundWords.end() || it->second.first != ctx.rawResults.size() - 1) {
			res.push_back({&holder_.getWordById(glbwordId).vids_, keyIt->first, proc, suffixes.virtual_word_len(suffixWordId)});
			res.idsCnt_ += holder_.getWordById(glbwordId).vids_.size();
			ctx.foundWords[glbwordId] = std::make_pair(ctx.rawResults.size() - 1, res.size() - 1);
			if (holder_.cfg_->logLevel >= LogTrace) {
				logPrintf(LogInfo, " matched %s '%s' of word '%s', %d vids, %d%%", suffixLen ? "suffix" : "prefix", keyIt->first, word,
						  holder_.getWordById(glbwordId).vids_.size(), proc);
			}
			matched++;
			vids += holder_.getWordById(glbwordId).vids_.size();
		} else {
			if (ctx.rawResults[it->second.first][it->second.second].proc_ < proc)
				ctx.rawResults[it->second.first][it->second.second].proc_ = proc;
			skipped++;
		}
	} while ((keyIt++).lcp() >= int(tmpstr.length()));
	if (holder_.cfg_->logLevel >= LogInfo) {
		logPrintf(LogInfo, "Lookup variant '%s' (%d%%), matched %d suffixes, with %d vids, skiped %d, excluded %d", tmpstr, variant.proc,
				  matched, vids, skipped, excludedCnt);
	}
}

template <typename IdCont>
template <bool mergeStatusesEmpty>
void Selecter<IdCont>::processVariants(FtSelectContext &ctx, const FtMergeStatuses::Statuses &mergeStatuses) {
	TextSearchResults &res = ctx.rawResults.back();

	for (const FtVariantEntry &variant : ctx.variants) {
		if (variant.opts.op == OpAnd) {
			ctx.foundWords.clear();
		}
		for (auto &step : holder_.steps) {
			processStepVariants<mergeStatusesEmpty>(ctx, step, variant, res, mergeStatuses);
		}
	}
}

template <typename IdCont>
void Selecter<IdCont>::processTypos(FtSelectContext &ctx, const FtDSLEntry &term) {
	TextSearchResults &res = ctx.rawResults.back();
	const auto maxTyposInWord = holder_.cfg_->MaxTyposInWord();
	const bool dontUseMaxTyposForBoth = maxTyposInWord != holder_.cfg_->maxTypos / 2;
	const size_t patternSize = utf16_to_utf8(term.pattern).size();
	for (auto &step : holder_.steps) {
		typos_context tctx[kMaxTyposInWord];
		const decltype(step.typosHalf_) *typoses[2]{&step.typosHalf_, &step.typosMax_};
		int matched = 0, skiped = 0, vids = 0;
		mktypos(tctx, term.pattern, maxTyposInWord, holder_.cfg_->maxTypoLen, [&](std::string_view typo, int level) {
			const int tcount = maxTyposInWord - level;
			for (const auto *typos : typoses) {
				const auto typoRng = typos->equal_range(typo);
				for (auto typoIt = typoRng.first; typoIt != typoRng.second; typoIt++) {
					WordIdType wordIdglb = typoIt->second;
					auto &step = holder_.GetStep(wordIdglb);

					auto wordIdSfx = holder_.GetSuffixWordId(wordIdglb, step);

					// bool virtualWord = suffixes_.is_word_virtual(wordId);
					uint8_t wordLength = step.suffixes_.word_len_at(wordIdSfx);
					int proc = kTypoProc - tcount * kTypoStepProc / std::max((wordLength - tcount) / 3, 1);
					auto it = ctx.foundWords.find(wordIdglb);
					if (it == ctx.foundWords.end() || it->second.first != ctx.rawResults.size() - 1) {
						res.push_back(
							{&holder_.getWordById(wordIdglb).vids_, typoIt->first, proc, step.suffixes_.virtual_word_len(wordIdSfx)});
						res.idsCnt_ += holder_.getWordById(wordIdglb).vids_.size();
						ctx.foundWords.emplace(wordIdglb, std::make_pair(ctx.rawResults.size() - 1, res.size() - 1));

						if (holder_.cfg_->logLevel >= LogTrace)
							logPrintf(LogInfo, " matched typo '%s' of word '%s', %d ids, %d%%", typoIt->first,
									  step.suffixes_.word_at(wordIdSfx), holder_.getWordById(wordIdglb).vids_.size(), proc);
						++matched;
						vids += holder_.getWordById(wordIdglb).vids_.size();
					} else {
						++skiped;
					}
				}
				if (dontUseMaxTyposForBoth && level == 1 && typo.size() != patternSize) return;
			}
		});
		if (holder_.cfg_->logLevel >= LogInfo)
			logPrintf(LogInfo, "Lookup typos, matched %d typos, with %d vids, skiped %d", matched, vids, skiped);
	}
}

static double bound(double k, double weight, double boost) noexcept { return (1.0 - weight) + k * boost * weight; }

template <typename IdCont>
void Selecter<IdCont>::debugMergeStep(const char *msg, int vid, float normBm25, float normDist, int finalRank, int prevRank) {
#ifdef REINDEX_FT_EXTRA_DEBUG
	if (holder_.cfg_->logLevel < LogTrace) return;

	logPrintf(LogInfo, "%s - '%s' (vid %d), bm25 %f, dist %f, rank %d (prev rank %d)", msg, holder_.vdocs_[vid].keyDoc, vid, normBm25,
			  normDist, finalRank, prevRank);
#else
	(void)msg;
	(void)vid;
	(void)normBm25;
	(void)normDist;
	(void)finalRank;
	(void)prevRank;
#endif
}

template <typename IdCont>
void Selecter<IdCont>::mergeItaration(const TextSearchResults &rawRes, index_t rawResIndex, FtMergeStatuses::Statuses &mergeStatuses,
									  std::vector<IDataHolder::MergeInfo> &merged, std::vector<MergedIdRel> &merged_rd,
									  std::vector<uint16_t> &idoffsets, std::vector<bool> &curExists, const bool hasBeenAnd,
									  const bool inTransaction, const RdxContext &rdxCtx) {
	const auto &vdocs = holder_.vdocs_;

	const size_t totalDocsCount = vdocs.size();
	const bool simple = idoffsets.empty();
	const auto op = rawRes.term.opts.op;

	curExists.clear();
	if (!simple || rawRes.size() > 1) {
		curExists.resize(totalDocsCount, false);
	}
	if (simple && rawRes.size() > 1) {
		idoffsets.resize(totalDocsCount);
	}
	for (auto &m_rd : merged_rd) {
		if (m_rd.next.Size()) m_rd.cur = std::move(m_rd.next);
	}

	for (auto &r : rawRes) {
		if (!inTransaction) ThrowOnCancel(rdxCtx);
		auto idf = IDF(totalDocsCount, r.vids_->size());

		for (auto &relid : *r.vids_) {
			static_assert((std::is_same_v<IdCont, IdRelVec> && std::is_same_v<decltype(relid), const IdRelType &>) ||
							  (std::is_same_v<IdCont, PackedIdRelVec> && std::is_same_v<decltype(relid), IdRelType &>),
						  "Expecting relid is movable for packed vector and not movable for simple vector");

			const int vid = relid.Id();
			index_t &vidStatus = mergeStatuses[vid];

			// Do not calc anithing if
			if ((vidStatus == FtMergeStatuses::kExcluded) | (hasBeenAnd & (vidStatus == 0))) {
				continue;
			}
			if (op == OpNot) {
				if (!simple & (vidStatus != 0)) {
					merged[idoffsets[vid]].proc = 0;
				}
				vidStatus = FtMergeStatuses::kExcluded;
				continue;
			}
			if (!vdocs[vid].keyEntry) continue;
			// Find field with max rank
			int field = 0;
			double normBm25 = 0.0, termRank = 0.0;
			bool dontSkipCurTermRank = false;
			auto termLenBoost = rawRes.term.opts.termLenBoost;
			h_vector<double, 4> ranksInFields;
			for (unsigned long long fieldsMask = relid.UsedFieldsMask(), f = 0; fieldsMask; ++f, fieldsMask >>= 1) {
#if defined(__GNUC__) || defined(__clang__)
				const auto bits = __builtin_ctzll(fieldsMask);
				f += bits;
				fieldsMask >>= bits;
#else
				while ((fieldsMask & 1) == 0) {
					++f;
					fieldsMask >>= 1;
				}
#endif
				assertrx(f < vdocs[vid].wordsCount.size());
				assertrx(f < rawRes.term.opts.fieldsOpts.size());
				const auto fboost = rawRes.term.opts.fieldsOpts[f].boost;
				if (fboost) {
					assertrx(f < holder_.cfg_->fieldsCfg.size());
					const auto &fldCfg = holder_.cfg_->fieldsCfg[f];
					// raw bm25
					const double bm25 = idf * bm25score(relid.WordsInField(f), vdocs[vid].mostFreqWordCount[f], vdocs[vid].wordsCount[f],
														holder_.avgWordsCount_[f]);

					// normalized bm25
					const double normBm25Tmp = bound(bm25, fldCfg.bm25Weight, fldCfg.bm25Boost);

					const double positionRank = bound(::pos2rank(relid.MinPositionInField(f)), fldCfg.positionWeight, fldCfg.positionBoost);

					termLenBoost = bound(rawRes.term.opts.termLenBoost, fldCfg.termLenWeight, fldCfg.termLenBoost);
					// final term rank calculation
					const double termRankTmp = fboost * r.proc_ * normBm25Tmp * rawRes.term.opts.boost * termLenBoost * positionRank;
					const bool needSumRank = rawRes.term.opts.fieldsOpts[f].needSumRank;
					if (termRankTmp > termRank) {
						if (dontSkipCurTermRank) {
							ranksInFields.push_back(termRank);
						}
						field = f;
						normBm25 = normBm25Tmp;
						termRank = termRankTmp;
						dontSkipCurTermRank = needSumRank;
					} else if (!dontSkipCurTermRank && needSumRank && termRank == termRankTmp) {
						field = f;
						normBm25 = normBm25Tmp;
						dontSkipCurTermRank = true;
					} else if (termRankTmp && needSumRank) {
						ranksInFields.push_back(termRankTmp);
					}
				}
			}
			if (!termRank) continue;
			if (holder_.cfg_->summationRanksByFieldsRatio > 0) {
				std::sort(ranksInFields.begin(), ranksInFields.end());
				double k = holder_.cfg_->summationRanksByFieldsRatio;
				for (auto r : ranksInFields) {
					termRank += (k * r);
					k *= holder_.cfg_->summationRanksByFieldsRatio;
				}
			}

			if (holder_.cfg_->logLevel >= LogTrace) {
				logPrintf(LogInfo, "Pattern %s, idf %f, termLenBoost %f", r.pattern, idf, termLenBoost);
			}

			// match of 2-rd, and next terms
			if (!simple && vidStatus) {
				assertrx(relid.Size());
				auto &curMerged = merged[idoffsets[vid]];
				auto &curMrd = merged_rd[idoffsets[vid]];
				assertrx(curMrd.cur.Size());

				// Calculate words distance
				int distance = 0;
				float normDist = 1;

				if (curMrd.qpos != rawRes.term.opts.qpos) {
					distance = curMrd.cur.Distance(relid, INT_MAX);

					// Normaized distance
					normDist = bound(1.0 / double(std::max(distance, 1)), holder_.cfg_->distanceWeight, holder_.cfg_->distanceBoost);
				}
				int finalRank = normDist * termRank;

				if (distance <= rawRes.term.opts.distance && (!curExists[vid] || finalRank > curMrd.rank)) {
					// distance and rank is better, than prev. update rank
					if (curExists[vid]) {
						curMerged.proc -= curMrd.rank;
						debugMergeStep("merged better score ", vid, normBm25, normDist, finalRank, curMrd.rank);
					} else {
						debugMergeStep("merged new ", vid, normBm25, normDist, finalRank, curMrd.rank);
						curMerged.matched++;
					}
					curMerged.proc += finalRank;
					if (needArea_) {
						for (auto pos : relid.Pos()) {
							if (!curMerged.holder->AddWord(pos.pos(), r.wordLen_, pos.field(), maxAreasInDoc_)) {
								break;
							}
						}
					}
					curMrd.rank = finalRank;
					curMrd.next = std::move(relid);
					curExists[vid] = true;
				} else {
					debugMergeStep("skiped ", vid, normBm25, normDist, finalRank, curMrd.rank);
				}
			}
			if (int(merged.size()) < holder_.cfg_->mergeLimit && !hasBeenAnd) {
				const bool currentlyAddedLessRankedMerge =
					!curExists.empty() && curExists[vid] && merged[idoffsets[vid]].proc < static_cast<int32_t>(termRank);
				if (!(simple && currentlyAddedLessRankedMerge) && vidStatus) continue;
				// match of 1-st term
				IDataHolder::MergeInfo info;
				info.id = vid;
				info.proc = termRank;
				info.matched = 1;
				info.field = field;
				if (needArea_) {
					info.holder.reset(new AreaHolder);
					info.holder->ReserveField(fieldSize_);
					for (auto pos : relid.Pos()) {
						info.holder->AddWord(pos.pos(), r.wordLen_, pos.field(), maxAreasInDoc_);
					}
				}
				if (vidStatus) {
					merged[idoffsets[vid]] = std::move(info);
				} else {
					merged.push_back(std::move(info));
					vidStatus = rawResIndex + 1;
					if (!curExists.empty()) {
						curExists[vid] = true;
						idoffsets[vid] = merged.size() - 1;
					}
				}
				if (simple) continue;
				// prepare for intersect with next terms
				merged_rd.push_back({IdRelType(std::move(relid)), IdRelType(), int(termRank), rawRes.term.opts.qpos});
			}
		}
	}
}

template <typename IdCont>
typename IDataHolder::MergeData Selecter<IdCont>::mergeResults(std::vector<TextSearchResults> &rawResults,
															   const std::vector<size_t> &synonymsBounds, bool inTransaction,
															   FtMergeStatuses::Statuses mergeStatuses, const RdxContext &rdxCtx) {
	const auto &vdocs = holder_.vdocs_;
	IDataHolder::MergeData merged;

	if (!rawResults.size() || !vdocs.size()) return merged;

	assertrx(FtMergeStatuses::kExcluded > rawResults.size());
	assertrx(mergeStatuses.size() == vdocs.size());
	std::vector<MergedIdRel> merged_rd;
	std::vector<uint16_t> idoffsets;

	int idsMaxCnt = 0;
	for (auto &rawRes : rawResults) {
		boost::sort::pdqsort(rawRes.begin(), rawRes.end(),
							 [](const TextSearchResult &lhs, const TextSearchResult &rhs) { return lhs.proc_ > rhs.proc_; });
		if (rawRes.term.opts.op == OpOr) idsMaxCnt += rawRes.idsCnt_;
	}

	merged.reserve(std::min(holder_.cfg_->mergeLimit, idsMaxCnt));

	if (rawResults.size() > 1) {
		idoffsets.resize(vdocs.size());
		merged_rd.reserve(std::min(holder_.cfg_->mergeLimit, idsMaxCnt));
	}
	std::vector<std::vector<bool>> exists(synonymsBounds.size() + 1);
	size_t curExists = 0;
	auto nextSynonymsBound = synonymsBounds.cbegin();
	bool hasBeenAnd = false;
	for (index_t i = 0, lastGroupStart = 0; i < rawResults.size(); ++i) {
		if (nextSynonymsBound != synonymsBounds.cend() && *nextSynonymsBound == i) {
			hasBeenAnd = false;
			++curExists;
			++nextSynonymsBound;
			if (nextSynonymsBound == synonymsBounds.cend()) {
				lastGroupStart = 0;
			} else {
				lastGroupStart = i;
			}
		}
		const auto &res = rawResults[i];
		mergeItaration(res, i, mergeStatuses, merged, merged_rd, idoffsets, exists[curExists], hasBeenAnd, inTransaction, rdxCtx);

		if (res.term.opts.op == OpAnd && !exists[curExists].empty()) {
			hasBeenAnd = true;
			for (auto &info : merged) {
				const auto vid = info.id;
				auto &vidStatus = mergeStatuses[vid];
				if (exists[curExists][vid] || vidStatus == FtMergeStatuses::kExcluded || vidStatus <= lastGroupStart || info.proc == 0) {
					continue;
				}
				bool matchSyn = false;
				for (size_t synGrpIdx : res.synonymsGroups) {
					assertrx(synGrpIdx < curExists);
					if (exists[synGrpIdx][vid]) {
						matchSyn = true;
						break;
					}
				}
				if (matchSyn) continue;
				info.proc = 0;
				vidStatus = 0;
			}
		}
	}
	if (holder_.cfg_->logLevel >= LogInfo) logPrintf(LogInfo, "Complex merge (%d patterns): out %d vids", rawResults.size(), merged.size());

	// Update full match rank
	for (size_t ofs = 0; ofs < merged.size(); ++ofs) {
		auto &m = merged[ofs];
		if (size_t(vdocs[m.id].wordsCount[m.field]) == rawResults.size()) {
			m.proc *= holder_.cfg_->fullMatchBoost;
		}
		if (merged.maxRank < m.proc) {
			merged.maxRank = m.proc;
		}
	}

	boost::sort::pdqsort(merged.begin(), merged.end(),
						 [](const IDataHolder::MergeInfo &lhs, const IDataHolder::MergeInfo &rhs) { return lhs.proc > rhs.proc; });

	return merged;
}

template class Selecter<PackedIdRelVec>;
template IDataHolder::MergeData Selecter<PackedIdRelVec>::Process<true>(FtDSLQuery &, bool, FtMergeStatuses::Statuses, const RdxContext &);
template IDataHolder::MergeData Selecter<PackedIdRelVec>::Process<false>(FtDSLQuery &, bool, FtMergeStatuses::Statuses, const RdxContext &);
template class Selecter<IdRelVec>;
template IDataHolder::MergeData Selecter<IdRelVec>::Process<true>(FtDSLQuery &, bool, FtMergeStatuses::Statuses, const RdxContext &);
template IDataHolder::MergeData Selecter<IdRelVec>::Process<false>(FtDSLQuery &, bool, FtMergeStatuses::Statuses, const RdxContext &);

}  // namespace reindexer
