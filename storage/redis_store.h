#ifndef CHAT_STORAGE_REDIS_STORE_H
#define CHAT_STORAGE_REDIS_STORE_H
#include <memory>
#include <string>
#include <vector>
namespace storage {
class RedisStore {
public:
    RedisStore();
    ~RedisStore();
    RedisStore(const RedisStore&) = delete;
    RedisStore& operator=(const RedisStore&) = delete;
    bool Append(const std::string& channel, const std::string& json, std::string* error);
    bool List(const std::string& channel, std::vector<std::string>* out, std::string* error);
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}
#endif
