#include "redis_store.h"
#include "../frame/config.h"
#include "../frame/socket_io.h"
#include <unistd.h>
#include <cerrno>
#include <cstdlib>
#include <cstring>
namespace storage {
using std::string;
using std::vector;
using std::to_string;
using frame::ReadAll;
using frame::SendAll;
using frame::kMaxFrame;
namespace {
int ConnectRedis() {
    return frame::Connect(frame::Env("CHAT_REDIS_HOST", "127.0.0.1"),
        frame::EnvInt("CHAT_REDIS_PORT", 6379, 1, 65535),
        frame::EnvInt("CHAT_IO_TIMEOUT_MS", 2000, 50, 60000));
}
}
struct RedisStore::Impl {
public:
    Impl() : fd_(-1) {}
    ~Impl() { if (fd_ >= 0) close(fd_); }

    bool Append(const string& channel, const string& json, string* err) {
        vector<string> x;
        string k = Key(channel);
        // Lua 将追加和裁剪作为一个原子操作，避免只写入而没有裁剪。
        const string script = "redis.call('RPUSH',KEYS[1],ARGV[1]); "
                              "redis.call('LTRIM',KEYS[1],-100,-1); return 1";
        return Exec(vector<string>{"EVAL", script, "1", k, json}, &x, err);
    }

    bool List(const string& channel, vector<string>* out, string* err) {
        return Exec(vector<string>{"LRANGE", Key(channel), "0", "-1"}, out, err);
    }

private:
    string Key(const string& c) {
        return "chat:channel:" + c + ":messages";
    }

    bool Line(string* out, string* err) {
        out->clear();
        char c;
        while (true) {
            if (!ReadAll(fd_, &c, 1)) {
                *err = "Redis 连接中断";
                return false;
            }
            if (c == '\r') {
                if (!ReadAll(fd_, &c, 1) || c != '\n') {
                    *err = "Redis RESP 格式错误";
                    return false;
                }
                return true;
            }
            if (out->size() >= 1024) { *err = "Redis RESP 行过长"; return false; }
            out->push_back(c);
        }
    }

    bool Number(const string& s, long long* v) {
        if (s.empty()) return false;
        errno = 0;
        char* end = nullptr;
        long long n = strtoll(s.c_str(), &end, 10);
        if (errno == ERANGE || end != s.c_str() + s.size()) return false;
        *v = n;
        return true;
    }

    bool Reply(vector<string>* out, string* err, int depth = 0) {
        if (depth > 8) { *err = "Redis RESP 嵌套过深"; return false; }
        char type;
        if (!ReadAll(fd_, &type, 1)) {
            *err = "Redis 连接中断";
            return false;
        }

        string line;
        if (!Line(&line, err)) return false;

        if (type == '+') {
            out->clear();
            return true;
        }
        if (type == ':') {
            out->assign(1, line);
            return true;
        }
        if (type == '-') {
            *err = "Redis: " + line;
            return false;
        }
        if (type == '$') {
            long long n;
            if (!Number(line, &n) || n < -1 || n > (long long)kMaxFrame) {
                *err = "Redis 长度错误";
                return false;
            }
            out->clear();
            if (n == -1) return true;
            string x((size_t)n, '\0');
            if (n && !ReadAll(fd_, &x[0], x.size())) {
                *err = "Redis 连接中断";
                return false;
            }
            char crlf[2];
            if (!ReadAll(fd_, crlf, 2) || memcmp(crlf, "\r\n", 2)) {
                *err = "Redis RESP 格式错误";
                return false;
            }
            out->push_back(x);
            return true;
        }
        if (type != '*') {
            *err = "未知 Redis 响应";
            return false;
        }

        long long count;
        if (!Number(line, &count) || count < 0 || count > 10000) {
            *err = "Redis 数组错误";
            return false;
        }
        out->clear();
        for (long long i = 0; i < count; ++i) {
            vector<string> one;
            if (!Reply(&one, err, depth + 1)) return false;
            if (!one.empty()) out->push_back(one[0]);
        }
        return true;
    }

    bool ExecOnce(const vector<string>& args, vector<string>* out, string* err) {
        string cmd = "*" + to_string(args.size()) + "\r\n";
        for (size_t i = 0; i < args.size(); ++i) {
            cmd += "$" + to_string(args[i].size()) + "\r\n" + args[i] + "\r\n";
        }
        return SendAll(fd_, cmd.data(), cmd.size()) && Reply(out, err);
    }

    bool Exec(const vector<string>& args, vector<string>* out, string* err) {
        if (fd_ < 0) fd_ = ConnectRedis();
        if (fd_ < 0) {
            *err = "无法连接 Redis";
            return false;
        }
        if (ExecOnce(args, out, err)) return true;
        close(fd_);
        fd_ = -1;
        if (err->empty()) *err = "Redis 连接中断或超时；写入结果可能未知";
        return false;
    }

    int fd_;
};

RedisStore::RedisStore() : impl_(new Impl) {}
RedisStore::~RedisStore() = default;
bool RedisStore::Append(const string& channel, const string& json, string* error) {
    error->clear(); return impl_->Append(channel, json, error);
}
bool RedisStore::List(const string& channel, vector<string>* out, string* error) {
    error->clear(); return impl_->List(channel, out, error);
}
}
