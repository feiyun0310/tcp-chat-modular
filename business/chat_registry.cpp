#include "chat_registry.h"
namespace business {
bool Registry::Login(const SessionPtr& session, const std::string& name,
                     const std::string& channel) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!session || !session->active.load() || users_.count(name)) return false;
    for (const auto& item : users_) if (item.second.session == session) return false;
    Info info;
    info.name = name;
    info.channel = channel;
    info.session = session;
    users_[name] = info;
    channels_[channel].insert(name);
    return true;
}
bool Registry::Ready(const SessionPtr& session) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& item : users_) {
        if (item.second.session == session && session->active.load()) {
            item.second.ready = true; return true;
        }
    }
    return false;
}
bool Registry::InfoFor(const SessionPtr& session, std::string* name,
                       std::string* channel) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& item : users_) {
        if (item.second.session == session) {
            *name = item.second.name;
            *channel = item.second.channel;
            return true;
        }
    }
    return false;
}
bool Registry::Logout(const SessionPtr& session, std::string* name,
                      std::string* channel) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = users_.begin(); it != users_.end(); ++it) {
        if (it->second.session != session) continue;
        *name = it->second.name;
        *channel = it->second.channel;
        auto c = channels_.find(*channel);
        if (c != channels_.end()) {
            c->second.erase(*name);
            if (c->second.empty()) channels_.erase(c);
        }
        users_.erase(it);
        return true;
    }
    return false;
}
std::vector<Registry::SessionPtr> Registry::Members(
    const std::string& channel, const std::string& except) {
    std::vector<SessionPtr> result;
    std::lock_guard<std::mutex> lock(mutex_);
    auto c = channels_.find(channel);
    if (c == channels_.end()) return result;
    for (const auto& name : c->second) {
        if (name == except) continue;
        auto u = users_.find(name);
        if (u != users_.end() && u->second.ready && u->second.session &&
            u->second.session->active.load()) {
            result.push_back(u->second.session);
        }
    }
    return result;
}
}
