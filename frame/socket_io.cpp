#include "socket_io.h"
#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <stdexcept>
namespace frame {
int Connect(const std::string& host, int port, int timeout_ms) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return -1;
    auto fail = [fd] { close(fd); return -1; };
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return fail();
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<std::uint16_t>(port));
    if (inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1) return fail();
    if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        if (errno != EINPROGRESS) return fail();
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        for (;;) {
            auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now()).count();
            if (left <= 0) return fail();
            pollfd p{fd, POLLOUT, 0};
            int result = poll(&p, 1, static_cast<int>(left));
            if (result < 0 && errno == EINTR) continue;
            if (result <= 0) return fail();
            int error = 0; socklen_t size = sizeof(error);
            if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &size) < 0 || error) return fail();
            break;
        }
    }
    timeval timeout{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    if (fcntl(fd, F_SETFL, flags) < 0 ||
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0 ||
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0) return fail();
    return fd;
}
bool SendAll(int fd, const void* data, std::size_t size) {
    const char* p = static_cast<const char*>(data);
    while (size) {
        ssize_t n = send(fd, p, size, MSG_NOSIGNAL);
        if (n > 0) { p += n; size -= static_cast<std::size_t>(n); }
        else if (n < 0 && errno == EINTR) continue;
        else return false;
    }
    return true;
}
bool ReadAll(int fd, void* data, std::size_t size) {
    char* p = static_cast<char*>(data);
    while (size) {
        ssize_t n = recv(fd, p, size, 0);
        if (n > 0) { p += n; size -= static_cast<std::size_t>(n); }
        else if (n < 0 && errno == EINTR) continue;
        else return false;
    }
    return true;
}
std::string EncodeFrame(const std::string& payload) {
    if (payload.size() > kMaxFrame) throw std::length_error("Frame exceeds 1 MiB");
    std::uint32_t length = htonl(static_cast<std::uint32_t>(payload.size()));
    std::string result(reinterpret_cast<const char*>(&length), sizeof(length));
    result += payload; return result;
}
bool WriteFrame(int fd, const std::string& payload) {
    if (payload.size() > kMaxFrame) return false;
    const auto bytes = EncodeFrame(payload);
    return SendAll(fd, bytes.data(), bytes.size());
}
bool ReadFrame(int fd, std::string* payload) {
    std::uint32_t wire = 0;
    if (!ReadAll(fd, &wire, sizeof(wire))) return false;
    auto size = ntohl(wire);
    if (size > kMaxFrame) return false;
    payload->assign(size, '\0');
    return !size || ReadAll(fd, &(*payload)[0], size);
}
int ExtractFrame(std::string* buffer, std::string* payload) {
    if (buffer->size() < 4) return 0;
    std::uint32_t wire; std::memcpy(&wire, buffer->data(), sizeof(wire));
    std::uint32_t size = ntohl(wire);
    if (size > kMaxFrame) return -1;
    if (buffer->size() < 4 + size) return 0;
    *payload = buffer->substr(4, size); buffer->erase(0, 4 + size); return 1;
}
}
