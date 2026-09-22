#include "frame/config.h"
#include "frame/socket_io.h"
#include <condition_variable>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

#include "protocol.pb.h"

using namespace std;
namespace {
atomic<bool> g_running(true);
mutex g_print;

bool SendProto(int fd, const chat::ClientEnvelope& message) {
    string payload;
    return message.SerializeToString(&payload) && frame::WriteFrame(fd, payload);
}
bool ReadProto(int fd, chat::ServerEnvelope* message) {
    string payload;
    return frame::ReadFrame(fd, &payload) && message->ParseFromString(payload);
}
string RequestId() {
    static atomic<unsigned long long> next(1);
    return to_string(chrono::steady_clock::now().time_since_epoch().count()) + "-" + to_string(next++);
}
string TimeText(int64_t ms) {
    time_t sec = static_cast<time_t>(ms / 1000);
    tm local{};
    localtime_r(&sec, &local);
    ostringstream out;
    out << put_time(&local, "%H:%M:%S");
    return out.str();
}
void Print(const chat::ServerEnvelope& msg) {
    lock_guard<mutex> lock(g_print);
    if (msg.has_chat_message()) {
        const chat::ChatMessage& x = msg.chat_message();
        cout << '[' << TimeText(x.timestamp_ms()) << "] " << x.sender_id() << ": " << x.content() << '\n';
    } else if (msg.has_system_message())
        cout << "[系统] " << msg.system_message().content() << '\n';
    else if (msg.has_error())
        cerr << "[错误] " << msg.error().code() << ": " << msg.error().message() << '\n';
}
}  // namespace

int RunClient() {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    int fd = frame::Connect(frame::Env("CHAT_LOGIC_HOST", "127.0.0.1"),
        frame::EnvInt("CHAT_LOGIC_PORT", 9999, 1, 65535),
        frame::EnvInt("CHAT_IO_TIMEOUT_MS", 2000, 50, 60000));
    if (fd < 0) { cerr << "连接 Logic Server 失败\n"; return 1; }
    struct SocketGuard { int fd; ~SocketGuard() { close(fd); } } guard{fd};
    string nickname, channel;
    cout << "昵称: ";
    getline(cin, nickname);
    cout << "频道: ";
    getline(cin, channel);
    chat::ClientEnvelope login;
    login.set_request_id(RequestId());
    login.mutable_login()->set_nickname(nickname);
    login.mutable_login()->set_channel_id(channel);
    if (!SendProto(fd, login)) return 1;
    chat::ServerEnvelope response;
    if (!ReadProto(fd, &response) || !response.has_login_response() || !response.login_response().success()) {
        Print(response);
        return 1;
    }
    cout << "登录成功，历史消息：\n";
    for (int i = 0; i < response.login_response().history_size(); ++i) {
        const chat::ChatMessage& x = response.login_response().history(i);
        cout << '[' << TimeText(x.timestamp_ms()) << "] " << x.sender_id() << ": " << x.content() << '\n';
    }
    // 登录完成后允许长时间空闲；退出等待由条件变量限定为 3 秒。
    timeval idle{0, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &idle, sizeof(idle));
    mutex done_mutex;
    condition_variable done;
    bool reader_done = false;
    string quit_id;
    thread reader([&] {
        while (g_running) {
            chat::ServerEnvelope msg;
            if (!ReadProto(fd, &msg)) {
                g_running = false;
                break;
            }
            Print(msg);
            bool quit_response = false;
            {
                lock_guard<mutex> lock(done_mutex);
                quit_response = !quit_id.empty() && msg.has_quit_response() && msg.request_id() == quit_id;
            }
            if (quit_response) break;
        }
        {
            lock_guard<mutex> lock(done_mutex);
            reader_done = true;
        }
        done.notify_one();
    });
    bool quitting = false;
    string line;
    while (g_running && getline(cin, line)) {
        chat::ClientEnvelope request;
        request.set_request_id(RequestId());
        if (line == "/quit") {
            request.mutable_quit();
            { lock_guard<mutex> lock(done_mutex); quit_id = request.request_id(); }
            quitting = SendProto(fd, request);
            break;
        }
        if (line.empty()) continue;
        request.mutable_send_message()->set_content(line);
        if (!SendProto(fd, request)) break;
    }
    if (!quitting && g_running) {
        chat::ClientEnvelope request;
        request.set_request_id(RequestId()); request.mutable_quit();
        { lock_guard<mutex> lock(done_mutex); quit_id = request.request_id(); }
        quitting = SendProto(fd, request);
    }
    if (quitting) {
        unique_lock<mutex> lock(done_mutex);
        if (!done.wait_for(lock, chrono::seconds(3), [&] { return reader_done; }))
            cerr << "等待退出响应超时\n";
    }
    g_running = false;
    shutdown(fd, SHUT_RDWR);
    reader.join();
    return 0;
}



int main() {
    try { return RunClient(); }
    catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
