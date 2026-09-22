#ifndef CHAT_FRAME_SOCKET_IO_H
#define CHAT_FRAME_SOCKET_IO_H
#include <cstddef>
#include <string>
namespace frame {
constexpr std::size_t kMaxFrame = 1024 * 1024;
int Connect(const std::string& host, int port, int timeout_ms);
bool SendAll(int fd, const void* data, std::size_t size);
bool ReadAll(int fd, void* data, std::size_t size);
bool WriteFrame(int fd, const std::string& payload);
bool ReadFrame(int fd, std::string* payload);
std::string EncodeFrame(const std::string& payload);
// 1：完整帧；0：等待更多数据；-1：帧超长。
int ExtractFrame(std::string* buffer, std::string* payload);
}
#endif
