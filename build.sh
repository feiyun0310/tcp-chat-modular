#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "$0")"
mkdir -p build/generated bin
protoc --cpp_out=build/generated --python_out=build/generated protocol.proto
compiler="${CXX:-g++}"
flags=(-std=c++17 -O2 -g -Wall -Wextra -Wpedantic -pthread -I. -Ibuild/generated)
if [[ -n "${SANITIZERS:-}" ]]; then
  flags+=(-O1 -fno-omit-frame-pointer "-fsanitize=${SANITIZERS}")
fi
frame=(frame/socket_io.cpp frame/tcp_server.cpp)
proto=(build/generated/protocol.pb.cc)
chat=(business/chat_registry.cpp business/chat_service.cpp)
"$compiler" "${flags[@]}" client.cpp frame/socket_io.cpp "${proto[@]}" -lprotobuf -o bin/client
"$compiler" "${flags[@]}" logicserver.cpp "${frame[@]}" "${chat[@]}" storage/data_proxy.cpp "${proto[@]}" -lprotobuf -o bin/logicserver
"$compiler" "${flags[@]}" dataserver.cpp "${frame[@]}" business/history_service.cpp storage/redis_store.cpp -o bin/dataserver
"$compiler" "${flags[@]}" tests/unit_tests.cpp "${frame[@]}" "${chat[@]}" "${proto[@]}" -lprotobuf -o bin/unit_tests
echo "Built client, logicserver, dataserver and unit_tests in bin/"
