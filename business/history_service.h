#ifndef CHAT_BUSINESS_HISTORY_SERVICE_H
#define CHAT_BUSINESS_HISTORY_SERVICE_H
#include <nlohmann/json.hpp>
#include "../storage/redis_store.h"
namespace business {
class HistoryService {
public:
    nlohmann::json Handle(const nlohmann::json& request, storage::RedisStore& redis);
    static nlohmann::json Error(const std::string& id, const std::string& code,
                                const std::string& text);
};
}
#endif
