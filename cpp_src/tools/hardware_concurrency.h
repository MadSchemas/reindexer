#pragma once

#include <thread>

#include <iostream>
#include "vendor/spdlog/fmt/fmt.h"

namespace reindexer {

// Wrapper to handle situation, when std::thread::hardware_concurrency returns 0.
inline unsigned hardware_concurrency() noexcept {
	std::cout << fmt::sprintf("std::hardware_concurrency: %d\n", std::thread::hardware_concurrency());
	return std::max(std::thread::hardware_concurrency(), 1u);
}

}  // namespace reindexer
