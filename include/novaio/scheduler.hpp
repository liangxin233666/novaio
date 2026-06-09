#pragma once

#include "novaio/concurrent_queues.hpp"
#include "novaio/io_engine.hpp"
#include "novaio/time_wheel.hpp"
#include <coroutine>
#include <array>
#include <mimalloc.h>
#include <poll.h>

namespace novaio {

class UniqueTask {
    struct Storage {
        void* ptr;
        void (*invoker)(void*);
        void (*deleter)(void*);
    };
public:
    UniqueTask() : storage_{nullptr, nullptr, nullptr} {}

    template<typename F>
    UniqueTask(F&& f) {
        using FuncType = std::decay_t<F>;
        auto* buf = mi_malloc(sizeof(FuncType));
        new (buf) FuncType(std::forward<F>(f));
        storage_.ptr = buf;
        storage_.invoker = [](void* p) { (*static_cast<FuncType*>(p))(); };
        storage_.deleter = [](void* p) {
            static_cast<FuncType*>(p)->~FuncType();
            mi_free(p);
        };
    }

    UniqueTask(UniqueTask&& other) noexcept : storage_(other.storage_) {
        other.storage_ = {nullptr, nullptr, nullptr};
    }

    UniqueTask& operator=(UniqueTask&& other) noexcept {
        if (this != &other) {
            cleanup();
            storage_ = other.storage_;
            other.storage_ = {nullptr, nullptr, nullptr};
        }
        return *this;
    }

    ~UniqueTask() { cleanup(); }

    void operator()() const { if (storage_.invoker) storage_.invoker(storage_.ptr); }
    explicit operator bool() const { return storage_.invoker != nullptr; }

private:
    void cleanup() { if (storage_.deleter) storage_.deleter(storage_.ptr); }
    Storage storage_;
};

extern thread_local mi_heap_t* current_thread_heap;
constexpr size_t BATCH_PIPELINE_DEPTH = 16;
constexpr uint64_t WAKEUP_USER_DATA = 0;

class Scheduler {
public:
    friend class IoEngine;

    explicit Scheduler(size_t id);
    ~Scheduler();

    void run();
    void schedule(std::coroutine_handle<> handle);

    bool push_inbox(UniqueTask task);

    size_t id() const { return id_; }
    static Scheduler* current() noexcept;

    TimeWheel& time_wheel() { return time_wheel_; }
    uint64_t current_time_ms() const;
    IoEngine& io_engine() noexcept { return io_engine_; }

    int ring_fd() const noexcept { return io_engine_.ring_fd(); }


private:
    size_t try_dequeue_batch(std::coroutine_handle<>* batch_out, size_t max_count);
    void process_io_events();
    void process_inbox();
    void setup_numa_and_affinity();

    size_t id_;

    LocalQueue local_queue_;
    SpscQueue<UniqueTask> inbox_;
    IoEngine io_engine_;

    std::coroutine_handle<> runnext_{nullptr};
    mi_heap_t* thread_heap_{nullptr};
    TimeWheel time_wheel_;
};

} // namespace novaio