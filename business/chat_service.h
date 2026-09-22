#ifndef TCP_GAME_CHAT_BUSINESS_CHAT_SERVICE_H
#define TCP_GAME_CHAT_BUSINESS_CHAT_SERVICE_H
#include <functional>
#include <memory>
#include <string>
#include "../frame/connection.h"
#include "protocol.pb.h"
#include "chat_registry.h"
namespace storage { class HistoryStore; }
namespace business {
class ChatService {
public:
    using SessionPtr = std::shared_ptr<frame::Session>;
    using SendCallback = std::function<void(
        const SessionPtr&, const chat::ServerEnvelope&)>;
    ChatService(Registry& registry, SendCallback sender);
    void Handle(const SessionPtr& session, const chat::ClientEnvelope& request,
                storage::HistoryStore& data);
    void OnDisconnected(const SessionPtr& session);
private:
    void OnLogin(const SessionPtr& session, const chat::ClientEnvelope& request,
                 storage::HistoryStore& data);
    void OnSendMessage(const SessionPtr& session,
                       const chat::ClientEnvelope& request,
                       storage::HistoryStore& data, const std::string& name,
                       const std::string& channel);
    void OnQuit(const SessionPtr& session, const chat::ClientEnvelope& request);
    void Send(const SessionPtr& session, const chat::ServerEnvelope& message);
    void SendError(const SessionPtr& session, const std::string& request_id,
                   const std::string& code, const std::string& text);
    void BroadcastSystem(const std::string& channel, const std::string& text,
                         const std::string& except = "");
    Registry& registry_;
    SendCallback sender_;
};
}
#endif
