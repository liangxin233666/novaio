#pragma once

#include <coroutine>
#include <exception>
#include <utility>
#include <mimalloc.h>
#include <new>

namespace novaio {

extern thread_local mi_heap_t* current_thread_heap;

template <typename T = void>
class [[clang::coro_await_elidable]] Task {
public:
    struct promise_type {
        T value_;
        std::exception_ptr exception_;
        std::coroutine_handle<> continuation_{nullptr};

        void* operator new(std::size_t size) noexcept { 
            return current_thread_heap ? mi_heap_malloc(current_thread_heap, size) : mi_malloc(size); 
        }
        void operator delete(void* ptr, std::size_t size) noexcept { mi_free_size(ptr, size); }

        Task get_return_object() noexcept { return Task{std::coroutine_handle<promise_type>::from_promise(*this)}; }
        std::suspend_always initial_suspend() noexcept { return {}; }
        
        struct FinalAwaiter {
            bool await_ready() const noexcept { return false; }
            template <typename Promise>
            std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> h) noexcept {
                if (auto cont = h.promise().continuation_) return cont;
                return std::noop_coroutine(); 
            }
            void await_resume() noexcept {}
        };
        FinalAwaiter final_suspend() noexcept { return {}; }

        template <typename U>
        void return_value(U&& value) { value_ = std::forward<U>(value); }
        void unhandled_exception() noexcept { exception_ = std::current_exception(); }
    };

    auto operator co_await() noexcept {
        struct Awaiter {
            std::coroutine_handle<promise_type> coro_;
            bool await_ready() const noexcept { return false; }
            std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiting_coro) noexcept {
                coro_.promise().continuation_ = awaiting_coro;
                return coro_; 
            }
            T await_resume() {
                if (coro_.promise().exception_) std::rethrow_exception(coro_.promise().exception_);
                return std::move(coro_.promise().value_);
            }
        };
        return Awaiter{coro_};
    }

    explicit Task(std::coroutine_handle<promise_type> h) noexcept : coro_(h) {}
    ~Task() { if (coro_) coro_.destroy(); }
    Task(Task&& t) noexcept : coro_(std::exchange(t.coro_, nullptr)) {}
    Task(const Task&) = delete;

private:
    std::coroutine_handle<promise_type> coro_;
};

struct DetachedTask {
    struct promise_type {
        void* operator new(std::size_t size) noexcept { 
            return current_thread_heap ? mi_heap_malloc(current_thread_heap, size) : mi_malloc(size); 
        }
        void operator delete(void* ptr, std::size_t size) noexcept { mi_free_size(ptr, size); }

        DetachedTask get_return_object() noexcept { return DetachedTask{std::coroutine_handle<promise_type>::from_promise(*this)}; }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() { std::terminate(); }
    };
    std::coroutine_handle<promise_type> coro_;
};

} // namespace novaio