// --- START OF FILE comutex.hpp ---
#pragma once

#include <atomic>
#include <coroutine>
#include <cstdint>
#include "novaio/scheduler.hpp"
#include "novaio/runtime.hpp"

namespace novaio {

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

        bool await_ready() const noexcept { return false; }
        bool await_suspend(std::coroutine_handle<> h) noexcept;
        void await_resume() const noexcept {}
    };

    Awaiter lock() noexcept { return Awaiter{*this}; }
    void unlock() noexcept;

private:
    static constexpr uintptr_t UNLOCKED = 0;
    static constexpr uintptr_t LOCKED_NO_WAITERS = 1;

    alignas(64) std::atomic<uintptr_t> state_{UNLOCKED}; 

    Awaiter* waiters_head_{nullptr};
};

} 