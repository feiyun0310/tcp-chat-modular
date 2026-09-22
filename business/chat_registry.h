#ifndef TCP_GAME_CHAT_BUSINESS_CHAT_REGISTRY_H
#define TCP_GAME_CHAT_BUSINESS_CHAT_REGISTRY_H
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "../frame/connection.h"
namespace business {
class Registry {
public:
    using SessionPtr = std::shared_ptr<frame::Session>;
    bool Login(const SessionPtr& session, const std::string& name,
               const std::string& channel);
    bool Ready(const SessionPtr& session);
    // 输出参数必须是有效指针。
    bool InfoFor(const SessionPtr& session, std::string* name, std::string* channel);
    bool Logout(const SessionPtr& session, std::string* name, std::string* channel);
    std::vector<SessionPtr> Members(const std::string& channel,
                                    const std::string& except = "");
private:
    struct Info {
        std::string name;
        std::string channel;
        SessionPtr session;
        bool ready = false;
    };
    std::mutex mutex_;
    std::unordered_map<std::string, Info> users_;
    std::unordered_map<std::string, std::unordered_set<std::string>> channels_;
};
}
#endif
