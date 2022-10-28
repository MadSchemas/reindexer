#include "json_parsing_test.h"

TEST_F(JSONParsingTest, EmptyDocument) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	Item item(rt.reindexer->NewItem(default_namespace));
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	err = item.FromJSON("\n");
	EXPECT_EQ(err.code(), errParseJson);

	err = item.FromJSON("\t");
	EXPECT_EQ(err.code(), errParseJson);

	err = item.FromJSON(" ");
	EXPECT_EQ(err.code(), errParseJson);
}

TEST_F(JSONParsingTest, Strings) {
	const std::vector<unsigned> lens = {0, 100, 8 < 10, 2 << 20, 8 << 20, 16 << 20, 32 << 20, 60 << 20};
	for (auto len : lens) {
		std::string strs[2];
		strs[0].resize(len / 2);
		std::fill(strs[0].begin(), strs[0].end(), 'a');
		strs[1].resize(len);
		std::fill(strs[1].begin(), strs[1].end(), 'b');

		std::string d("{\"id\":1,\"str0\":\"" + strs[0] + "\",\"str1\":\"" + strs[1] + "\",\"val\":999}");
		reindexer::span<char> data(d);
		try {
			gason::JsonParser parser;
			auto root = parser.Parse(data, nullptr);
			EXPECT_EQ(root["id"].As<int>(), 1) << len;
			auto rstr = root["str0"].As<std::string_view>();
			EXPECT_EQ(rstr, strs[0]) << len;
			rstr = root["str1"].As<std::string_view>();
			EXPECT_EQ(rstr, strs[1]) << len;
			EXPECT_EQ(root["val"].As<int>(), 999) << len;
		} catch (gason::Exception& e) {
			EXPECT_TRUE(false) << e.what();
		}
	}
}
