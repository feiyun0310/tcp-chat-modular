#include "frame/config.h"
#include "frame/tcp_server.h"
#include "business/chat_service.h"
#include "storage/data_proxy.h"
#include <csignal>
#include <iostream>
#include <memory>
#include <vector>
namespace { volatile std::sig_atomic_t stopped = 0; void Stop(int) { stopped = 1; } }
int main() {
    std::signal(SIGINT, Stop); std::signal(SIGTERM, Stop);
    try {
        frame::ServerOptions options;
        options.host = frame::Env("CHAT_LOGIC_BIND", "127.0.0.1");
        options.port = frame::EnvInt("CHAT_LOGIC_PORT", 9999, 1, 65535);
        options.workers = frame::EnvInt("CHAT_WORKERS", 4, 1, 32);
        std::vector<std::unique_ptr<storage::DataProxy>> data;
        for (int i = 0; i < options.workers; ++i) data.emplace_back(new storage::DataProxy);
        business::Registry registry;
        frame::TcpServer* transport = nullptr;
        business::ChatService service(registry,
            [&](const frame::SessionPtr& session, const chat::ServerEnvelope& message) {
                std::string payload;
                if (message.SerializeToString(&payload)) transport->Send(session, payload);
                else transport->Close(session);
            });
        frame::TcpServer server(options,
            [&](std::size_t worker, const frame::SessionPtr& session, const std::string& payload) {
                chat::ClientEnvelope request;
                if (!request.ParseFromString(payload) || request.request_id().empty() ||
                    request.request_id().size() > 128 || request.payload_case() == chat::ClientEnvelope::PAYLOAD_NOT_SET) {
                    transport->Close(session); return;
                }
                service.Handle(session, request, *data[worker]);
            },
            [&](const frame::SessionPtr& session) { service.OnDisconnected(session); });
        transport = &server;
        return server.Run(stopped);
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
