#ifndef CHAT_FRAME_BLOCKING_QUEUE_H
#define CHAT_FRAME_BLOCKING_QUEUE_H
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <utility>
namespace frame {
// Stop 拒绝新任务，消费者仍可排空已有任务。
template<class T> class Queue {
public:
    explicit Queue(std::size_t capacity = 128) : capacity_(capacity) {}
    bool Push(T item) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopped_ || items_.size() >= capacity_) return false;
        items_.push_back(std::move(item)); ready_.notify_one(); return true;
    }
    // 仅供关闭事件使用；调用方限制连接数，每个连接最多一次。
    bool PushControl(T item) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopped_) return false;
        items_.push_back(std::move(item)); ready_.notify_one(); return true;
    }
    bool Pop(T* item) {
        std::unique_lock<std::mutex> lock(mutex_);
        ready_.wait(lock, [this] { return stopped_ || !items_.empty(); });
        return Take(item);
    }
    bool TryPop(T* item) {
        std::lock_guard<std::mutex> lock(mutex_); return Take(item);
    }
    void Stop() {
        std::lock_guard<std::mutex> lock(mutex_); stopped_ = true; ready_.notify_all();
    }
private:
    bool Take(T* item) {
        if (items_.empty()) return false;
        *item = std::move(items_.front()); items_.pop_front(); return true;
    }
    std::size_t capacity_;
    bool stopped_ = false;
    std::deque<T> items_;
    std::mutex mutex_;
    std::condition_variable ready_;
};
}
#endif
