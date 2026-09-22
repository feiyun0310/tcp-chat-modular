#include "frame/blocking_queue.h"
#include "frame/socket_io.h"
#include "business/chat_service.h"
#include "storage/history_store.h"
#include <arpa/inet.h>
#include <cassert>
#include <future>
#include <iostream>
#include <stdexcept>
#include <vector>
using Json = nlohmann::json;
struct MemoryHistory : storage::HistoryStore {
    bool fail = false;
    bool malformed = false;
    std::vector<chat::ChatMessage> saved;
    bool History(const std::string&, const std::string&, Json* out, std::string* error) override {
        if (fail) { *error = "offline"; return false; }
        *out = Json{{"messages", malformed ? Json("bad") : Json::array()}};
        return true;
    }
    bool Append(const std::string&, const chat::ChatMessage& message, std::string* error) override {
        if (fail) { *error = "offline"; return false; }
        saved.push_back(message); return true;
    }
};
chat::ClientEnvelope Login(const std::string& id, const std::string& name, const std::string& room) {
    chat::ClientEnvelope request; request.set_request_id(id);
    request.mutable_login()->set_nickname(name); request.mutable_login()->set_channel_id(room);
    return request;
}
void QueueTest() {
    frame::Queue<int> queue(2);
    assert(queue.Push(1)); assert(queue.Push(2)); assert(!queue.Push(3));
    queue.Stop(); assert(!queue.Push(4));
    int value; assert(queue.Pop(&value) && value == 1); assert(queue.Pop(&value) && value == 2);
    assert(!queue.Pop(&value));
    frame::Queue<int> empty;
    auto waiter = std::async(std::launch::async, [&] { int v; return empty.Pop(&v); });
    empty.Stop(); assert(!waiter.get());
    std::cout << "PASS bounded queue, FIFO, stop/drain and wakeup\n";
}
void FrameTest() {
    const std::string binary("a\0b", 3);
    const auto encoded = frame::EncodeFrame(binary);
    std::string input, output;
    for (std::size_t i = 0; i < encoded.size(); ++i) {
        input += encoded[i];
        assert(frame::ExtractFrame(&input, &output) == (i + 1 == encoded.size() ? 1 : 0));
    }
    assert(output == binary && input.empty());
    input = frame::EncodeFrame("one") + frame::EncodeFrame("two");
    assert(frame::ExtractFrame(&input, &output) == 1 && output == "one");
    assert(frame::ExtractFrame(&input, &output) == 1 && output == "two");
    std::uint32_t wire = htonl(frame::kMaxFrame + 1);
    input.assign(reinterpret_cast<const char*>(&wire), 4);
    assert(frame::ExtractFrame(&input, &output) == -1);
    std::cout << "PASS fragmented, coalesced, binary and oversized frames\n";
}
void BusinessTest() {
    business::Registry registry;
    using Sent = std::pair<frame::Session*, chat::ServerEnvelope>;
    std::vector<Sent> sent;
    business::ChatService service(registry, [&](const business::ChatService::SessionPtr& s,
                                               const chat::ServerEnvelope& message) {
        sent.emplace_back(s.get(), message);
    });
    MemoryHistory store;
    auto a = std::make_shared<frame::Session>(10, 1);
    auto b = std::make_shared<frame::Session>(11, 2);
    auto c = std::make_shared<frame::Session>(12, 3);
    service.Handle(a, Login("1", "alice", "room"), store);
    assert(sent.back().second.has_login_response());
    service.Handle(b, Login("2", "alice", "room"), store);
    assert(sent.back().second.error().code() == "NICKNAME_TAKEN");
    service.Handle(b, Login("3", "bob", "room"), store);
    service.Handle(c, Login("4", "carol", "elsewhere"), store);
    sent.clear();
    chat::ClientEnvelope request;
    request.set_request_id("5"); request.mutable_send_message()->set_content("hello");
    service.Handle(a, request, store);
    assert(store.saved.size() == 1 && store.saved[0].sender_id() == "alice");
    assert(sent.size() == 3); // 一个确认 + 同频道两个用户的广播
    for (const auto& item : sent) assert(item.first != c.get());
    store.fail = true; sent.clear(); service.Handle(a, request, store);
    assert(sent.size() == 1 && sent[0].second.error().code() == "DATA_UNAVAILABLE");
    auto d = std::make_shared<frame::Session>(13, 4);
    service.Handle(d, Login("6", "retry", "room"), store);
    std::string name, channel;
    assert(!registry.InfoFor(d, &name, &channel));
    store.fail = false; store.malformed = true;
    service.Handle(d, Login("7", "retry", "room"), store);
    assert(!registry.InfoFor(d, &name, &channel));
    store.malformed = false;
    service.Handle(d, Login("8", "retry", "room"), store);
    assert(registry.InfoFor(d, &name, &channel));
    request.Clear(); request.set_request_id("9"); request.mutable_quit();
    service.Handle(a, request, store); assert(!registry.InfoFor(a, &name, &channel));
    b->active = false; service.OnDisconnected(b);
    assert(!registry.InfoFor(b, &name, &channel));
    assert(!registry.Login(b, "dead", "room"));
    std::cout << "PASS login, nickname conflict, broadcast isolation, store failure, rollback, quit and disconnect\n";
}
int main() { QueueTest(); FrameTest(); BusinessTest(); }
