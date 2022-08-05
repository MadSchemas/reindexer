#pragma once

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <tuple>

#include <iostream>
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/variant.h"
#include "core/query/query.h"
#include "core/reindexer.h"
#include "gtests/tests/gtest_cout.h"
#include "reindexertestapi.h"
#include "servercontrol.h"
#include "tools/errors.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "vendor/utf8cpp/utf8.h"

using reindexer::Error;
using reindexer::Item;
using reindexer::Variant;
using reindexer::VariantArray;
using reindexer::Query;
using reindexer::QueryEntry;
using reindexer::QueryResults;
using reindexer::Reindexer;

class ReindexerApi : public ::testing::Test {
protected:
	void SetUp() {}

	void TearDown() {}

public:
	ReindexerApi() {}

	void DefineNamespaceDataset(const string &ns, std::initializer_list<const IndexDeclaration> fields) {
		rt.DefineNamespaceDataset(ns, fields);
	}
	void DefineNamespaceDataset(Reindexer &rx, const string &ns, std::initializer_list<const IndexDeclaration> fields) {
		rt.DefineNamespaceDataset(rx, ns, fields);
	}
	Item NewItem(std::string_view ns) { return rt.NewItem(ns); }

	Error Commit(std::string_view ns) { return rt.Commit(ns); }
	void Upsert(std::string_view ns, Item &item) { rt.Upsert(ns, item); }

	void PrintQueryResults(const std::string &ns, const QueryResults &res) { rt.PrintQueryResults(ns, res); }
	string PrintItem(Item &item) { return rt.PrintItem(item); }

	std::string RandString() { return rt.RandString(); }
	std::string RandLikePattern() { return rt.RandLikePattern(); }
	std::string RuRandString() { return rt.RuRandString(); }
	vector<int> RandIntVector(size_t size, int start, int range) { return rt.RandIntVector(size, start, range); }

	struct QueryWatcher {
		~QueryWatcher() {
			if (::testing::Test::HasFailure()) {
				reindexer::WrSerializer ser;
				q.GetSQL(ser);
				TEST_COUT << "Failed query dest: " << ser.Slice() << std::endl;
			}
		}

		const Query &q;
	};

public:
	const string default_namespace = "test_namespace";
	ReindexerTestApi<reindexer::Reindexer> rt;

protected:
	void initializeDefaultNs() {
		auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled());
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->AddIndex(default_namespace, {"value", "text", "string", IndexOpts()});
		ASSERT_TRUE(err.ok()) << err.what();
	}
};

class CanceledRdxContext : public reindexer::IRdxCancelContext {
public:
	reindexer::CancelType GetCancelType() const noexcept override { return reindexer::CancelType::Explicit; }
	bool IsCancelable() const noexcept override { return true; }
};

class DummyRdxContext : public reindexer::IRdxCancelContext {
public:
	reindexer::CancelType GetCancelType() const noexcept override { return reindexer::CancelType::None; }
	bool IsCancelable() const noexcept override { return false; }
};

class FakeRdxContext : public reindexer::IRdxCancelContext {
public:
	reindexer::CancelType GetCancelType() const noexcept override { return reindexer::CancelType::None; }
	bool IsCancelable() const noexcept override { return true; }
};
