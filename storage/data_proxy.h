#ifndef CHAT_STORAGE_DATA_PROXY_H
#define CHAT_STORAGE_DATA_PROXY_H
#include "history_store.h"
namespace storage {
class DataProxy : public HistoryStore {
public:
    DataProxy();
    ~DataProxy() override;
    DataProxy(const DataProxy&) = delete;
    DataProxy& operator=(const DataProxy&) = delete;
    bool History(const std::string& id, const std::string& channel,
                 nlohmann::json* out, std::string* error) override;
    bool Append(const std::string& id, const chat::ChatMessage& message,
                std::string* error) override;
private:
    bool Request(const nlohmann::json& request, nlohmann::json* out, std::string* error);
    void Reset();
    int fd_ = -1;
    std::string host_;
    int port_;
    int timeout_ms_;
};
}
#endif
