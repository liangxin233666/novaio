#pragma once

#include <atomic>
#include <coroutine>
#include <cstdint>

namespace novaio {

class Scheduler;

class CoMutex {
public:
    CoMutex() noexcept = default;
    ~CoMutex() = default;

    CoMutex(const CoMutex&) = delete;
    CoMutex& operator=(const CoMutex&) = delete;

    struct Awaiter {
        CoMutex& mutex_;
        std::coroutine_handle<> awaiting_coroutine_{nullptr};
        Scheduler* scheduler_{nullptr}; 
        Awaiter* next_{nullptr};

        explicit Awaiter(CoMutex& m) noexcept : mutex_(m) {}

        bool await_ready() const noexcept {
            uintptr_t expected = UNLOCKED;
            return mutex_.state_.compare_exchange_strong(
                expected, LOCKED_NO_WAITERS,
                std::memory_order_acquire,
                std::memory_order_relaxed);
        }

        bool await_suspend(std::coroutine_handle<> h) noexcept;
        void await_resume() const noexcept {}
    };

    Awaiter lock() noexcept { return Awaiter{*this}; }
    void unlock() noexcept;

private:
    static constexpr uintptr_t UNLOCKED = 0;
    static constexpr uintptr_t LOCKED_NO_WAITERS = 1;
    
    alignas(64) std::atomic<uintptr_t> state_{UNLOCKED}; 
};

} 