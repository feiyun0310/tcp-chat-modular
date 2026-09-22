#include "chat_service.h"
#include "../storage/history_store.h"
#include <chrono>
#include <cstdint>
#include <exception>
#include <stdexcept>
#include <utility>
#include <vector>
#include <nlohmann/json.hpp>
namespace {
std::int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
}
namespace business {
using std::string;
using std::vector;
using std::shared_ptr;
using frame::Session;
using Json = nlohmann::json;
ChatService::ChatService(Registry& registry, SendCallback sender)
    : registry_(registry), sender_(std::move(sender)) {
    if (!sender_) throw std::invalid_argument("ChatService 需要发送回调");
}

// 总入口只负责分派请求；非登录请求先校验登录状态。
void ChatService::Handle(const SessionPtr& session,
                         const chat::ClientEnvelope& request,
                         storage::HistoryStore& data) {
    if (!session || !session->active.load()) return;
    try {
        if (request.has_login()) {
            OnLogin(session, request, data);
            return;
        }
        string name, channel;
        if (!registry_.InfoFor(session, &name, &channel)) {
            SendError(session, request.request_id(), "NOT_LOGGED_IN", "请先登录");
            return;
        }
        if (request.has_send_message()) {
            OnSendMessage(session, request, data, name, channel);
            return;
        }
        if (request.has_quit()) {
            OnQuit(session, request);
            return;
        }
        SendError(session, request.request_id(), "UNKNOWN_REQUEST", "未知请求");
    } catch (const std::exception& e) {
        SendError(session, request.request_id(), "INTERNAL", e.what());
    }
}

// 检查昵称/频道 → 登记用户 → 查询历史 → 响应 → 广播加入通知。
void ChatService::OnLogin(const SessionPtr& session,
                          const chat::ClientEnvelope& r,
                          storage::HistoryStore& data) {
    string name, channel;
    const chat::LoginRequest& q = r.login();
    if (q.nickname().empty() || q.nickname().size() > 64 || q.channel_id().empty() || q.channel_id().size() > 64) {
        SendError(session, r.request_id(), "INVALID_LOGIN", "昵称或频道不合法");
        return;
    }
    if (registry_.InfoFor(session, &name, &channel)) {
        SendError(session, r.request_id(), "ALREADY_LOGGED_IN", "此连接已登录");
        return;
    }
    if (!registry_.Login(session, q.nickname(), q.channel_id())) {
        SendError(session, r.request_id(), "NICKNAME_TAKEN", "昵称已被占用");
        return;
    }
    Json result;
    string err;
    try {
        if (!data.History(r.request_id(), q.channel_id(), &result, &err)) {
            registry_.Logout(session, &name, &channel);
            SendError(session, r.request_id(), "DATA_UNAVAILABLE", err);
            return;
        }
        chat::ServerEnvelope response;
        response.set_request_id(r.request_id());
        auto* login = response.mutable_login_response();
        login->set_success(true);
        login->set_message("登录成功");
        const Json& messages = result.at("messages");
        if (!messages.is_array()) throw std::runtime_error("Invalid history array");
        for (const auto& item : messages) {
            auto* message = login->add_history();
            message->set_channel_id(item.at("channel_id").get<string>());
            message->set_sender_id(item.at("sender_id").get<string>());
            message->set_content(item.at("content").get<string>());
            message->set_timestamp_ms(item.at("timestamp_ms").get<std::int64_t>());
        }
        // 登录响应先入发送队列，再允许其他线程把此连接当作广播接收者。
        Send(session, response);
        if (registry_.Ready(session)) {
            BroadcastSystem(q.channel_id(), q.nickname() + " 加入了频道", q.nickname());
        }
    } catch (const std::exception&) {
        registry_.Logout(session, &name, &channel);
        SendError(session, r.request_id(), "DATA_UNAVAILABLE", "历史消息响应无效");
    }
}

// 检查内容 → 保存消息 → 确认 → 广播给频道成员。
void ChatService::OnSendMessage(const SessionPtr& session,
                                const chat::ClientEnvelope& r,
                                storage::HistoryStore& data,
                                const string& name, const string& channel) {
    if (r.send_message().content().empty() || r.send_message().content().size() > 8192) {
        SendError(session, r.request_id(), "INVALID_MESSAGE", "消息不合法");
        return;
    }
    chat::ChatMessage m;
    m.set_channel_id(channel);
    m.set_sender_id(name);
    m.set_content(r.send_message().content());
    m.set_timestamp_ms(NowMs());
    string err;
    if (!data.Append(r.request_id(), m, &err)) {
        SendError(session, r.request_id(), "DATA_UNAVAILABLE", err);
        return;
    }
    chat::ServerEnvelope ack;
    ack.set_request_id(r.request_id());
    ack.mutable_send_message_response()->set_success(true);
    ack.mutable_send_message_response()->set_message("消息已保存");
    Send(session, ack);
    chat::ServerEnvelope event;
    *event.mutable_chat_message() = m;
    vector<shared_ptr<Session> > all = registry_.Members(channel);
    for (size_t i = 0; i < all.size(); ++i) Send(all[i], event);
    return;
}

void ChatService::OnQuit(const SessionPtr& session,
                         const chat::ClientEnvelope& request) {
    chat::ServerEnvelope response;
    response.set_request_id(request.request_id());
    response.mutable_quit_response()->set_success(true);
    Send(session, response);
    string name, channel;
    if (registry_.Logout(session, &name, &channel)) {
        BroadcastSystem(channel, name + " 离开了频道");
    }
}
void ChatService::OnDisconnected(const SessionPtr& session) {
    if (!session) return;
    string name, channel;
    if (registry_.Logout(session, &name, &channel)) {
        BroadcastSystem(channel, name + " 连接已断开");
    }
}
void ChatService::Send(const SessionPtr& session,
                       const chat::ServerEnvelope& message) {
    if (!session || !session->active.load()) return;
    sender_(session, message);
}
void ChatService::SendError(const SessionPtr& session,
                            const string& request_id, const string& code,
                            const string& text) {
    chat::ServerEnvelope response;
    response.set_request_id(request_id);
    response.mutable_error()->set_code(code);
    response.mutable_error()->set_message(text);
    Send(session, response);
}
void ChatService::BroadcastSystem(const string& channel, const string& text,
                                  const string& except) {
    chat::ServerEnvelope event;
    event.mutable_system_message()->set_content(text);
    for (const auto& member : registry_.Members(channel, except)) Send(member, event);
}
}
