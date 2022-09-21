#include "base_fixture.h"

#include <benchmark/benchmark.h>
#include <functional>
#include <random>
#include <string>
#include <thread>

using std::string;

using reindexer::Error;

using benchmark::RegisterBenchmark;

Error BaseFixture::Initialize() {
	assert(db_);
	return db_->AddNamespace(nsdef_);
}

void BaseFixture::RegisterAllCases() {
	Register("Insert" + std::to_string(id_seq_->Count()), &BaseFixture::Insert, this)->Iterations(1);
	Register("Update", &BaseFixture::Update, this)->Iterations(id_seq_->Count());
}

std::string BaseFixture::RandString() {
	string res;
	uint8_t len = rand() % 20 + 4;
	res.resize(len);
	for (int i = 0; i < len; ++i) {
		int f = rand() % letters.size();
		res[i] = letters[f];
	}
	return res;
}

// FIXTURES

void BaseFixture::Insert(State& state) {
	benchmark::AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		for (int i = 0; i < id_seq_->Count(); ++i) {
			auto item = MakeItem();
			if (!item.Status().ok()) state.SkipWithError(item.Status().what().c_str());

			auto err = db_->Insert(nsdef_.name, item);
			if (!err.ok()) state.SkipWithError(err.what().c_str());
			state.SetItemsProcessed(state.items_processed() + 1);
		}
	}

	auto err = db_->Commit(nsdef_.name);
	if (!err.ok()) state.SkipWithError(err.what().c_str());
}

void BaseFixture::Update(benchmark::State& state) {
	benchmark::AllocsTracker allocsTracker(state);
	id_seq_->Reset();
	for (auto _ : state) {
		auto item = MakeItem();
		if (!item.Status().ok()) state.SkipWithError(item.Status().what().c_str());

		auto err = db_->Update(nsdef_.name, item);
		if (!err.ok()) state.SkipWithError(err.what().c_str());

		if (item.GetID() < 0) {
			auto e = Error(errConflict, "Item not exists [id = '%d']", item["id"].As<int>());
			state.SkipWithError(e.what().c_str());
		}
		state.SetItemsProcessed(state.items_processed() + 1);
	}
	auto err = db_->Commit(nsdef_.name);
	if (!err.ok()) state.SkipWithError(err.what().c_str());
}

void BaseFixture::WaitForOptimization() {
	for (;;) {
		reindexer::Query q("#memstats");
		q.Where("name", CondEq, nsdef_.name);
		reindexer::QueryResults res;
		auto e = db_->Select(q, res);
		assert(e.ok());
		assert(res.Count() == 1);
		assert(res.IsLocal());
		auto item = res.ToLocalQr().begin().GetItem(false);
		if (item["optimization_completed"].As<bool>() == true) {
			break;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
	}
}
