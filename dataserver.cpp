#include "frame/config.h"
#include "frame/socket_io.h"
#include "frame/tcp_server.h"
#include "business/history_service.h"
#include <csignal>
#include <iostream>
#include <memory>
#include <vector>
namespace { volatile std::sig_atomic_t stopped = 0; void Stop(int) { stopped = 1; } }
int main() {
    std::signal(SIGINT, Stop); std::signal(SIGTERM, Stop);
    try {
        frame::ServerOptions options;
        options.host = frame::Env("CHAT_DATA_BIND", "127.0.0.1");
        options.port = frame::EnvInt("CHAT_DATA_PORT", 8888, 1, 65535);
        options.workers = frame::EnvInt("CHAT_WORKERS", 4, 1, 32);
        std::vector<std::unique_ptr<storage::RedisStore>> stores;
        for (int i = 0; i < options.workers; ++i) stores.emplace_back(new storage::RedisStore);
        business::HistoryService service;
        frame::TcpServer* transport = nullptr;
        frame::TcpServer server(options,
            [&](std::size_t worker, const frame::SessionPtr& session, const std::string& payload) {
                auto request = nlohmann::json::parse(payload, nullptr, false);
                auto result = request.is_discarded()
                    ? business::HistoryService::Error("", "INVALID_JSON", "无效 JSON")
                    : service.Handle(request, *stores[worker]);
                auto bytes = result.dump();
                if (bytes.size() > frame::kMaxFrame)
                    bytes = business::HistoryService::Error(result.value("request_id", std::string()),
                        "RESPONSE_TOO_LARGE", "历史响应超过帧大小限制").dump();
                transport->Send(session, bytes);
            }, [](const frame::SessionPtr&) {});
        transport = &server;
        return server.Run(stopped);
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
