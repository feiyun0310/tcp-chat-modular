#include "tcp_server.h"
#include "blocking_queue.h"
#include "socket_io.h"
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
namespace frame {
struct TcpServer::Impl {
    struct Task { SessionPtr session; std::string payload; bool disconnected; };
    struct Outbound { SessionPtr session; std::string bytes; };
    ServerOptions options;
    Handler handler;
    DisconnectHandler disconnected;
    int listener = -1, epoll = -1, wakeup = -1;
    std::uint64_t next_id = 1;
    std::unordered_map<int, Connection> connections;
    // pending_sessions 包含已经关闭但尚未完成清理的连接，防止关闭事件积压无界增长。
    std::atomic<std::size_t> pending_sessions{0};
    std::vector<std::unique_ptr<Queue<Task>>> queues;
    std::vector<std::thread> workers;
    Queue<Outbound> outgoing{256};
    Impl(ServerOptions o, Handler h, DisconnectHandler d)
        : options(std::move(o)), handler(std::move(h)), disconnected(std::move(d)) {
        if (options.workers < 1 || !handler || !disconnected)
            throw std::invalid_argument("Invalid server options/callbacks");
        for (int i = 0; i < options.workers; ++i)
            queues.emplace_back(new Queue<Task>(options.queue_capacity));
    }
    ~Impl() { Shutdown(); }
    std::size_t Shard(const SessionPtr& s) const { return s->id % queues.size(); }
    void Wake() {
        std::uint64_t one = 1;
        while (write(wakeup, &one, sizeof(one)) < 0 && errno == EINTR) {}
        // EAGAIN 表示计数器已有唤醒；Run 每 100ms 也会检查请求关闭的连接。
    }
    void Close(const SessionPtr& s) {
        s->close_requested.store(true); Wake();
    }
    void Send(const SessionPtr& s, const std::string& payload) {
        if (!s || !s->active.load()) return;
        if (payload.size() > kMaxFrame || !outgoing.Push(Outbound{s, EncodeFrame(payload)})) {
            Close(s); return;
        }
        Wake();
    }
    bool Events(int fd, std::uint32_t events, int operation = EPOLL_CTL_MOD) {
        epoll_event event{}; event.events = events; event.data.fd = fd;
        return epoll_ctl(epoll, operation, fd, &event) == 0;
    }
    void Drop(int fd) {
        auto it = connections.find(fd);
        if (it == connections.end()) return;
        auto session = it->second.session;
        session->active.store(false);
        epoll_ctl(epoll, EPOLL_CTL_DEL, fd, nullptr);
        close(fd); connections.erase(it);
        // 和该连接的请求进入同一 FIFO：清理不可能跑到正在执行的登录前面。
        queues[Shard(session)]->PushControl(Task{session, "", true});
    }
    void Worker(std::size_t index) {
        Task task;
        while (queues[index]->Pop(&task)) {
            try {
                if (task.disconnected) disconnected(task.session);
                else if (task.session->active.load()) handler(index, task.session, task.payload);
            } catch (const std::exception& e) {
                std::cerr << "worker: " << e.what() << '\n'; Close(task.session);
            } catch (...) { Close(task.session); }
            if (task.disconnected) --pending_sessions;
        }
    }
    bool Read(Connection& connection) {
        char bytes[8192];
        // 每轮读入有预算，避免一个高流量连接长期占据网络线程。
        std::size_t budget = 256 * 1024;
        while (budget) {
            ssize_t n = recv(connection.session->fd, bytes, sizeof(bytes), 0);
            if (n > 0) {
                budget -= static_cast<std::size_t>(n);
                connection.input.append(bytes, static_cast<std::size_t>(n));
                for (;;) {
                    std::string payload;
                    int result = ExtractFrame(&connection.input, &payload);
                    if (result < 0) return false;
                    if (!result) break;
                    if (!queues[Shard(connection.session)]->Push(
                            Task{connection.session, std::move(payload), false})) return false;
                }
                continue;
            }
            if (!n) return false;
            if (errno == EINTR) continue;
            return errno == EAGAIN || errno == EWOULDBLOCK;
        }
        return true;
    }
    bool Flush(Connection& connection) {
        while (!connection.output.empty()) {
            ssize_t n = send(connection.session->fd, connection.output.data(),
                             connection.output.size(), MSG_NOSIGNAL);
            if (n > 0) { connection.output.erase(0, static_cast<std::size_t>(n)); continue; }
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
            return false;
        }
        return Events(connection.session->fd, EPOLLIN | EPOLLRDHUP);
    }
    void Drain() {
        Outbound item;
        while (outgoing.TryPop(&item)) {
            auto it = connections.find(item.session->fd);
            // fd 可以复用；必须同时核对 Session 对象身份。
            if (it == connections.end() || it->second.session != item.session) continue;
            if (it->second.output.size() + item.bytes.size() > 4 * kMaxFrame) {
                Drop(item.session->fd); continue;
            }
            it->second.output += item.bytes;
            if (!Events(item.session->fd, EPOLLIN | EPOLLRDHUP | EPOLLOUT)) Drop(item.session->fd);
        }
        std::vector<int> closed;
        for (const auto& entry : connections)
            if (entry.second.session->close_requested.load()) closed.push_back(entry.first);
        for (int fd : closed) Drop(fd);
    }
    void Accept() {
        for (int accepted = 0; accepted < 64; ++accepted) {
            int fd = accept4(listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (fd < 0) {
                if (errno == EINTR) { --accepted; continue; }
                break;
            }
            if (pending_sessions.load() >= options.max_connections) { close(fd); continue; }
            if (!Events(fd, EPOLLIN | EPOLLRDHUP, EPOLL_CTL_ADD)) { close(fd); continue; }
            SessionPtr session(new Session(fd, next_id++));
            ++pending_sessions;
            connections.emplace(fd, Connection{session, "", ""});
        }
    }
    void Shutdown() {
        if (listener >= 0) { close(listener); listener = -1; }
        while (!connections.empty()) Drop(connections.begin()->first);
        for (auto& queue : queues) queue->Stop();
        for (auto& worker : workers) if (worker.joinable()) worker.join();
        // 工作线程不会再写 eventfd 后才关闭它。
        outgoing.Stop();
        if (wakeup >= 0) { close(wakeup); wakeup = -1; }
        if (epoll >= 0) { close(epoll); epoll = -1; }
    }
    int Run(const volatile std::sig_atomic_t& stop) {
        listener = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        epoll = epoll_create1(EPOLL_CLOEXEC);
        wakeup = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        int yes = 1;
        sockaddr_in address{}; address.sin_family = AF_INET;
        address.sin_port = htons(static_cast<std::uint16_t>(options.port));
        if (listener < 0 || epoll < 0 || wakeup < 0 ||
            inet_pton(AF_INET, options.host.c_str(), &address.sin_addr) != 1 ||
            setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0 ||
            bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 ||
            listen(listener, 128) < 0 || !Events(listener, EPOLLIN, EPOLL_CTL_ADD) ||
            !Events(wakeup, EPOLLIN, EPOLL_CTL_ADD)) {
            std::cerr << "Server startup failed: " << options.host << ':' << options.port << '\n';
            Shutdown(); return 1;
        }
        for (std::size_t i = 0; i < queues.size(); ++i)
            workers.emplace_back([this, i] { Worker(i); });
        std::cout << "Listening " << options.host << ':' << options.port
                  << " workers=" << options.workers << std::endl;
        epoll_event events[128];
        int status = 0;
        while (!stop) {
            int count = epoll_wait(epoll, events, 128, 100);
            if (count < 0) { if (errno == EINTR) continue; status = 1; break; }
            for (int i = 0; i < count; ++i) {
                int fd = events[i].data.fd;
                auto flags = events[i].events;
                if (fd == listener) { Accept(); continue; }
                if (fd == wakeup) {
                    std::uint64_t value;
                    while (read(wakeup, &value, sizeof(value)) > 0) {}
                    continue;
                }
                auto it = connections.find(fd);
                if (it == connections.end()) continue;
                bool alive = !(flags & (EPOLLERR | EPOLLHUP | EPOLLRDHUP));
                if (alive && (flags & EPOLLIN)) alive = Read(it->second);
                if (alive && (flags & EPOLLOUT)) alive = Flush(it->second);
                if (!alive) Drop(fd);
            }
            Drain();
        }
        Shutdown(); return status;
    }
};
TcpServer::TcpServer(ServerOptions options, Handler handler, DisconnectHandler disconnected)
    : impl_(new Impl(std::move(options), std::move(handler), std::move(disconnected))) {}
TcpServer::~TcpServer() = default;
int TcpServer::Run(const volatile std::sig_atomic_t& stop) { return impl_->Run(stop); }
void TcpServer::Send(const SessionPtr& session, const std::string& payload) { impl_->Send(session, payload); }
void TcpServer::Close(const SessionPtr& session) { impl_->Close(session); }
}
