#pragma once

#include <time.h>
#include <cctype>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>
#include "core/indexopts.h"
#include "core/keyvalue/variant.h"
#include "core/type_consts.h"
#include "tools/customhash.h"
#include "tools/errors.h"

namespace reindexer {

std::string escapeString(std::string_view str);
std::string unescapeString(std::string_view str);
KeyValueType detectValueType(std::string_view value);
Variant stringToVariant(std::string_view value);

static inline bool isalpha(char c) { return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'); }
static inline bool isdigit(char c) { return (c >= '0' && c <= '9'); }
static inline char tolower(char c) { return (c >= 'A' && c <= 'Z') ? c + 'a' - 'A' : c; }
std::string toLower(std::string_view src);
inline std::string_view skipSpace(std::string_view str) {
	size_t i = 0;
	for (; i < str.size() && std::isspace(str[i]); ++i)
		;
	return str.substr(i);
}

template <typename Container>
Container& split(const typename Container::value_type& str, const std::string& delimiters, bool trimEmpty, Container& tokens) {
	tokens.resize(0);

	for (size_t pos, lastPos = 0;; lastPos = pos + 1) {
		pos = str.find_first_of(delimiters, lastPos);
		if (pos == std::string::npos) {
			pos = str.length();
			if (pos != lastPos || !trimEmpty) tokens.push_back(str.substr(lastPos, pos - lastPos));
			break;
		} else if (pos != lastPos || !trimEmpty)
			tokens.push_back(str.substr(lastPos, pos - lastPos));
	}
	return tokens;
}

void split(const std::string& utf8Str, std::wstring& utf16str, std::vector<std::wstring>& words);
void split(std::string_view utf8Str, std::wstring& utf16str, std::vector<std::wstring>& words, const std::string& extraWordSymbols);
void split(std::string_view str, std::string& buf, std::vector<const char*>& words, const std::string& extraWordSymbols);
size_t calcUTf8Size(const char* s, size_t size, size_t limit);
size_t calcUTf8SizeEnd(const char* end, int pos, size_t limit);

int getUTF8StringCharactersCount(std::string_view str);

class Word2PosHelper {
public:
	Word2PosHelper(std::string_view data, const std::string& extraWordSymbols);
	std::pair<int, int> convert(int wordPos, int endPos);

protected:
	std::string_view data_;
	int lastWordPos_, lastOffset_;
	const std::string& extraWordSymbols_;
};

int collateCompare(std::string_view lhs, std::string_view rhs, const CollateOpts& collateOpts);

std::wstring utf8_to_utf16(std::string_view src);
std::string utf16_to_utf8(const std::wstring& src);
std::wstring& utf8_to_utf16(std::string_view src, std::wstring& dst);
std::string& utf16_to_utf8(const std::wstring& src, std::string& dst);

void check_for_replacement(wchar_t& ch);
void check_for_replacement(uint32_t& ch);
bool is_number(std::string_view str);

int fast_strftime(char* buf, const tm* tm);
std::string urldecode2(std::string_view str);

int stoi(std::string_view sl);
int64_t stoll(std::string_view sl);

bool validateObjectName(std::string_view name, bool allowSpecialChars) noexcept;
LogLevel logLevelFromString(const std::string& strLogLevel);
StrictMode strictModeFromString(const std::string& strStrictMode);
std::string_view strictModeToString(StrictMode mode);

bool iequals(std::string_view lhs, std::string_view rhs);
bool checkIfStartsWith(std::string_view src, std::string_view pattern, bool casesensitive = false);
bool isPrintable(std::string_view str);
bool isBlank(std::string_view token);

Error cursosPosToBytePos(std::string_view str, size_t line, size_t charPos, size_t& bytePos);

std::string randStringAlph(size_t len);

struct nocase_equal_str {
	using is_transparent = void;

	bool operator()(std::string_view lhs, std::string_view rhs) const { return iequals(lhs, rhs); }
	bool operator()(std::string_view lhs, const std::string& rhs) const { return iequals(lhs, rhs); }
	bool operator()(const std::string& lhs, std::string_view rhs) const { return iequals(lhs, rhs); }
	bool operator()(const std::string& lhs, const std::string& rhs) const { return iequals(lhs, rhs); }
};

struct nocase_hash_str {
	using is_transparent = void;
	size_t operator()(std::string_view hs) const { return collateHash(hs, CollateASCII); }
	size_t operator()(const std::string& hs) const { return collateHash(hs, CollateASCII); }
};

struct equal_str {
	using is_transparent = void;

	bool operator()(std::string_view lhs, const std::string& rhs) const { return lhs == rhs; }
	bool operator()(const std::string& lhs, std::string_view rhs) const { return rhs == lhs; }
	bool operator()(const std::string& lhs, const std::string& rhs) const { return lhs == rhs; }
};

struct hash_str {
	using is_transparent = void;
	size_t operator()(std::string_view hs) const { return collateHash(hs, CollateNone); }
	size_t operator()(const std::string& hs) const { return collateHash(hs, CollateNone); }
};

inline void deepCopy(std::string& dst, const std::string& src) {
	dst.resize(src.size());
	std::memcpy(&dst[0], &src[0], src.size());
}

}  // namespace reindexer
