#pragma once

#include <coroutine>
#include <cstdint>
#include <mimalloc.h>
#include <utility>

namespace novaio {

extern thread_local mi_heap_t* current_thread_heap;

struct IoContext {
    int res{0};
    uint32_t flags{0};
    std::coroutine_handle<> coro_{nullptr};
    bool cancelled_{false};

    void* operator new(size_t size) {
        return current_thread_heap ? mi_heap_malloc(current_thread_heap, size) : mi_malloc(size);
    }
    void operator delete(void* p) { mi_free(p); }
};

struct IoResult {
    int res;
    uint32_t flags;
};

struct SafeIoAwaiter {
    IoContext* ctx_;

    explicit SafeIoAwaiter(IoContext* ctx) : ctx_(ctx) {}
    ~SafeIoAwaiter() {
        if (ctx_) ctx_->cancelled_ = true;
    }

    SafeIoAwaiter(const SafeIoAwaiter&) = delete;
    SafeIoAwaiter(SafeIoAwaiter&& other) noexcept : ctx_(std::exchange(other.ctx_, nullptr)) {}

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept { ctx_->coro_ = h; }
    IoResult await_resume() {
        IoResult ret = {ctx_->res, ctx_->flags};
        delete ctx_;
        ctx_ = nullptr;
        return ret;
    }
};

} // namespace novaio