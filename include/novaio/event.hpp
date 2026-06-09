#pragma once
#include <atomic>
#include <coroutine>
#include <cstdint>
#include <mutex>
#include "novaio/runtime.hpp"
#include "novaio/scheduler.hpp"

namespace novaio {


class Event {
public:
    Event() noexcept = default;

    struct Awaiter {
        Event& event_;
        std::coroutine_handle<> awaiting_coroutine_{nullptr};
        Scheduler* scheduler_{nullptr};
        Awaiter* next_{nullptr};

        bool await_ready() const noexcept {
            return event_.signaled_.load(std::memory_order_acquire);
        }

        bool await_suspend(std::coroutine_handle<> h) noexcept {
            awaiting_coroutine_ = h;
            scheduler_ = Scheduler::current();

            std::lock_guard lock(event_.mutex_);
            if (event_.signaled_.load(std::memory_order_acquire)) {
                return false;
            }

            next_ = event_.waiters_;
            event_.waiters_ = this;
            return true;
        }

        void await_resume() const noexcept {}
    };

    void set() noexcept {
        if (signaled_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }

        Awaiter* waiters = nullptr;
        {
            std::lock_guard lock(mutex_);
            waiters = waiters_;
            waiters_ = nullptr;
        }

        while (waiters) {
            Awaiter* next = waiters->next_;
            resume_waiter(waiters->scheduler_, waiters->awaiting_coroutine_);
            waiters = next;
        }
    }

    auto operator co_await() noexcept {
        return Awaiter{*this};
    }

private:
    static void resume_waiter(Scheduler* scheduler, std::coroutine_handle<> coro) noexcept {
        if (!coro) {
            return;
        }

        auto* current = Scheduler::current();
        if (scheduler && current == scheduler) {
            scheduler->schedule(coro);
        } else if (scheduler) {
            Runtime::get().dispatch_on(scheduler->id(), UniqueTask([coro]() {
                Scheduler::current()->schedule(coro);
            }));
        } else {
            coro.resume();
        }
    }

    std::atomic<bool> signaled_{false};
    std::mutex mutex_;
    Awaiter* waiters_{nullptr};
};

} // namespace novaio
