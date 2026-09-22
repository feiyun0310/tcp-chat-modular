#ifndef CHAT_STORAGE_HISTORY_STORE_H
#define CHAT_STORAGE_HISTORY_STORE_H
#include <string>
#include <nlohmann/json.hpp>
#include "protocol.pb.h"
namespace storage {
// 业务依赖这个接口；正式运行用 DataProxy，单元测试用内存替身。
class HistoryStore {
public:
    virtual ~HistoryStore() = default;
    virtual bool History(const std::string& id, const std::string& channel,
                         nlohmann::json* out, std::string* error) = 0;
    virtual bool Append(const std::string& id, const chat::ChatMessage& message,
                        std::string* error) = 0;
};
}
#endif
