#pragma once

#include <atomic>
#include <string>
#include "core/type_consts.h"

#include <iostream>
#include "tools/assertrx.h"

namespace reindexer {
namespace cluster {

#if defined(rtfmt) || defined(rtstr) || defined(logTrace) || defined(logInfo) || defined(logWarn) || defined(logError)
static_assert(false, "Macros conflict");
#endif

#define rtfmt(f, ...) return fmt::sprintf("[cluster:%s] " f, logModuleName(), __VA_ARGS__)
#define rtstr(f) return fmt::sprintf("[cluster:%s] " f, logModuleName())
#define logTrace(f, ...) log_.Trace([&] { rtfmt(f, __VA_ARGS__); })
#define logInfo(f, ...) log_.Info([&] { rtfmt(f, __VA_ARGS__); })
#define logWarn(f, ...) log_.Warn([&] { rtfmt(f, __VA_ARGS__); })
#define logError(f, ...) log_.Error([&] { rtfmt(f, __VA_ARGS__); })

class Logger {
public:
	Logger(LogLevel minOutputLogLevel = LogInfo) noexcept : minOutputLogLevel_(minOutputLogLevel) {}

	void SetLevel(LogLevel l) noexcept { level_.store(l, std::memory_order_relaxed); }
	LogLevel GetLevel() const noexcept { return level_.load(std::memory_order_relaxed); }

	template <typename F>
	void Error(F&& f) const {
		Log(LogError, std::forward<F>(f));
	}
	template <typename F>
	void Warn(F&& f) const {
		Log(LogWarning, std::forward<F>(f));
	}
	template <typename F>
	void Info(F&& f) const {
		Log(LogInfo, std::forward<F>(f));
	}
	template <typename F>
	void Trace(F&& f) const {
		Log(LogTrace, std::forward<F>(f));
	}
	template <typename F>
	void Log(LogLevel l, F&& f) const {
		if (l <= GetLevel()) {
			try {
			std::string str = f();
			if (!str.empty()) {
				const auto outLevel = minOutputLogLevel_ < l ? minOutputLogLevel_ : l;
				print(outLevel, str);
			}
			} catch (std::exception& e) {
				std::cout << "!!!!!" << e.what() << std::endl;
				assertrx(false);
			}
			catch (...) {
				std::cout << "!!!!!<unknown error>" << std::endl;
				assertrx(false);
			}
		}
	}

private:
	void print(LogLevel l, std::string& str) const;

	std::atomic<LogLevel> level_;
	const LogLevel minOutputLogLevel_;
};

}  // namespace cluster
}  // namespace reindexer
