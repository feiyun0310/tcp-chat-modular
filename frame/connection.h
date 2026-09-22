#ifndef CHAT_FRAME_CONNECTION_H
#define CHAT_FRAME_CONNECTION_H
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
namespace frame {
struct Session {
    int fd;
    std::uint64_t id;
    std::atomic<bool> active{true};
    std::atomic<bool> close_requested{false};
    Session(int socket_fd, std::uint64_t session_id) : fd(socket_fd), id(session_id) {}
};
// 网络线程独占缓冲区；Session 身份在网络与工作线程间共享。
struct Connection {
    std::shared_ptr<Session> session;
    std::string input;
    std::string output;
};
}
#endif
