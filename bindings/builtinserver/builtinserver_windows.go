// +build windows
//go:generate cmd /c cd ..\.. && mkdir build & cd build && cmake -G "MinGW Makefiles" -DLINK_RESOURCES=On -DCMAKE_BUILD_TYPE=Release .. && cmake --build . --target reindexer reindexer_server_library -- -j4

package builtin

// #cgo CXXFLAGS: -std=c++11 -g -O2 -Wall -Wpedantic -Wextra -I../../cpp_src
// #cgo CFLAGS: -std=c99 -g -O2 -Wall -Wpedantic -Wno-unused-variable -I../../cpp_src
// #cgo LDFLAGS: -L${SRCDIR}/../../build/cpp_src/ -L${SRCDIR}/../../build/cpp_src/server/ -lreindexer -lreindexer_server_library -lreindexer_server_resources -lleveldb -lsnappy -g -lshlwapi -ldbghelp -lws2_32
import "C"
