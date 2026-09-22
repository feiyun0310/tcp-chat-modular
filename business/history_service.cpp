#include "history_service.h"
#include <cstdint>
namespace business {
using Json = nlohmann::json;
Json HistoryService::Error(const std::string& id, const std::string& code, const std::string& text) {
    return Json{{"version",1},{"request_id",id},{"ok",false},{"data",Json::object()},
                {"error",{{"code",code},{"message",text}}}};
}
namespace {
Json Ok(const std::string& id, Json data) {
    return Json{{"version",1},{"request_id",id},{"ok",true},{"data",data},{"error",nullptr}};
}
}
Json HistoryService::Handle(const Json& request, storage::RedisStore& redis) {
    std::string id;
    try {
        // 类型错误也返回 INVALID_REQUEST，异常处理不再次读取错误类型的字段。
        if (!request.is_object()) return Error(id, "INVALID_REQUEST", "请求必须是对象");
        if (request.contains("request_id") && request["request_id"].is_string())
            id = request["request_id"].get<std::string>();
        if (request.at("version").get<int>() != 1 || id.empty() || id.size() > 128)
            return Error(id, "INVALID_REQUEST", "version 或 request_id 不合法");
        auto action = request.at("action").get<std::string>();
        auto channel = request.at("channel_id").get<std::string>();
        if (channel.empty() || channel.size() > 64)
            return Error(id, "INVALID_CHANNEL", "频道不合法");
        std::string error;
        if (action == "append_message") {
            auto sender = request.at("sender_id").get<std::string>();
            auto content = request.at("content").get<std::string>();
            if (!request.at("timestamp_ms").is_number_integer())
                return Error(id, "INVALID_MESSAGE", "timestamp_ms 必须是整数");
            auto timestamp = request.at("timestamp_ms").get<std::int64_t>();
            if (sender.empty() || sender.size() > 64 || content.empty() || content.size() > 8192 || timestamp < 0)
                return Error(id, "INVALID_MESSAGE", "消息字段不合法");
            Json message{{"channel_id",channel},{"sender_id",sender},{"content",content},{"timestamp_ms",timestamp}};
            if (!redis.Append(channel, message.dump(), &error)) return Error(id,"REDIS_UNAVAILABLE",error);
            return Ok(id, Json{{"stored",true}});
        }
        if (action == "get_channel_history") {
            std::vector<std::string> raw;
            if (!redis.List(channel, &raw, &error)) return Error(id,"REDIS_UNAVAILABLE",error);
            Json messages = Json::array();
            for (const auto& text : raw) {
                Json item = Json::parse(text, nullptr, false);
                if (item.is_discarded() || !item.is_object()) return Error(id,"CORRUPT_DATA","历史消息损坏");
                messages.push_back(item);
            }
            return Ok(id, Json{{"messages",messages}});
        }
        return Error(id,"UNKNOWN_ACTION","未知 action");
    } catch (const Json::exception&) { return Error(id,"INVALID_REQUEST","字段缺失或类型错误"); }
}
}
