#include "cluster/logger.h"
#include "tools/logger.h"

#include <iostream>

namespace reindexer {
namespace cluster {

void Logger::print(LogLevel , std::string& str) const { str.append("\n"); std::cout << str; } ///logPrint(l, &str[0]); }

}  // namespace cluster
}  // namespace reindexer
