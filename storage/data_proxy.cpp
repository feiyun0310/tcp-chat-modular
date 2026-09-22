#include "data_proxy.h"
#include "../frame/config.h"
#include "../frame/socket_io.h"
#include <unistd.h>
namespace storage {
using Json = nlohmann::json;
DataProxy::DataProxy()
    : host_(frame::Env("CHAT_DATA_HOST", "127.0.0.1")),
      port_(frame::EnvInt("CHAT_DATA_PORT", 8888, 1, 65535)),
      timeout_ms_(frame::EnvInt("CHAT_IO_TIMEOUT_MS", 2000, 50, 60000)) {}
DataProxy::~DataProxy() { Reset(); }
void DataProxy::Reset() { if (fd_ >= 0) close(fd_); fd_ = -1; }
bool DataProxy::History(const std::string& id, const std::string& channel,
                        Json* out, std::string* error) {
    return Request(Json{{"version", 1}, {"request_id", id},
        {"action", "get_channel_history"}, {"channel_id", channel}}, out, error);
}
bool DataProxy::Append(const std::string& id, const chat::ChatMessage& message,
                       std::string* error) {
    Json ignored;
    return Request(Json{{"version", 1}, {"request_id", id}, {"action", "append_message"},
        {"channel_id", message.channel_id()}, {"sender_id", message.sender_id()},
        {"content", message.content()}, {"timestamp_ms", message.timestamp_ms()}}, &ignored, error);
}
bool DataProxy::Request(const Json& request, Json* out, std::string* error) {
    if (fd_ < 0) fd_ = frame::Connect(host_, port_, timeout_ms_);
    if (fd_ < 0) { *error = "连接 Data Server 失败"; return false; }
    std::string response;
    // 写入可能已经成功但响应丢失。不能自动重试并声称只保存一次。
    if (!frame::WriteFrame(fd_, request.dump()) || !frame::ReadFrame(fd_, &response)) {
        Reset(); *error = "Data Server 连接中断或超时；写入结果可能未知"; return false;
    }
    try {
        Json result = Json::parse(response);
        if (!result.is_object() || result.value("version", 0) != 1 ||
            result.value("request_id", std::string()) != request.at("request_id").get<std::string>()) {
            Reset(); *error = "Data Server 响应不匹配"; return false;
        }
        if (!result.at("ok").get<bool>()) {
            *error = result.at("error").at("message").get<std::string>(); return false;
        }
        *out = result.at("data"); error->clear(); return true;
    } catch (const std::exception&) {
        Reset(); *error = "Data Server 响应格式错误"; return false;
    }
}
}
