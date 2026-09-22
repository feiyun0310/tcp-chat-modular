#ifndef CHAT_FRAME_TCP_SERVER_H
#define CHAT_FRAME_TCP_SERVER_H
#include "connection.h"
#include <csignal>
#include <functional>
#include <memory>
#include <string>
namespace frame {
using SessionPtr = std::shared_ptr<Session>;
struct ServerOptions {
    std::string host = "127.0.0.1";
    int port = 9999;
    int workers = 4;
    std::size_t max_connections = 1024;
    std::size_t queue_capacity = 128;
};
// 框架只认识连接和字节串，不依赖 JSON、Protobuf、昵称或 Redis。
class TcpServer {
public:
    using Handler = std::function<void(std::size_t, const SessionPtr&, const std::string&)>;
    using DisconnectHandler = std::function<void(const SessionPtr&)>;
    TcpServer(ServerOptions options, Handler handler, DisconnectHandler disconnected);
    ~TcpServer();
    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;
    int Run(const volatile std::sig_atomic_t& stop);
    void Send(const SessionPtr& session, const std::string& payload);
    void Close(const SessionPtr& session);
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}
#endif
