#include "novaio/comutex.hpp"

namespace novaio {

bool CoMutex::Awaiter::await_suspend(std::coroutine_handle<> h) noexcept {
    awaiting_coroutine_ = h;
    scheduler_ = Scheduler::current();

    uintptr_t old_state = mutex_.state_.load(std::memory_order_acquire);

    while (true) {
        if (old_state == UNLOCKED) {
            if (mutex_.state_.compare_exchange_weak(
                    old_state, LOCKED_NO_WAITERS,
                    std::memory_order_acquire,
                    std::memory_order_relaxed)) {
                return false;
            }
        } else {

            next_ = reinterpret_cast<Awaiter*>(old_state == LOCKED_NO_WAITERS ? 0 : old_state);
            if (mutex_.state_.compare_exchange_weak(
                    old_state, reinterpret_cast<uintptr_t>(this),
                    std::memory_order_release,
                    std::memory_order_relaxed)) {
                return true;
            }
        }
    }
}

void CoMutex::unlock() noexcept {
    if (waiters_head_ == nullptr) {

        uintptr_t old_state = state_.exchange(LOCKED_NO_WAITERS, std::memory_order_acquire);

        if (old_state == LOCKED_NO_WAITERS) {

            if (state_.compare_exchange_strong(
                    old_state, UNLOCKED,
                    std::memory_order_release,
                    std::memory_order_relaxed)) {
                return;
            }

            old_state = state_.exchange(LOCKED_NO_WAITERS, std::memory_order_acquire);
        }


        Awaiter* current = reinterpret_cast<Awaiter*>(old_state);
        Awaiter* prev = nullptr;
        while (current != nullptr) {
            Awaiter* next = current->next_;
            current->next_ = prev;
            prev = current;
            current = next;
        }
        waiters_head_ = prev;
    }

    if (!waiters_head_) return;


    Awaiter* to_resume = waiters_head_;
    waiters_head_ = to_resume->next_;

    auto* current_sched = Scheduler::current();
    auto* target_sched = to_resume->scheduler_;

    if (target_sched == current_sched && current_sched != nullptr) {
        current_sched->schedule(to_resume->awaiting_coroutine_);
    } else if (target_sched != nullptr) {
        uintptr_t coro_addr = reinterpret_cast<uintptr_t>(to_resume->awaiting_coroutine_.address());
        Runtime::get().notify_target_ring(target_sched->ring_fd(), coro_addr | TAG_CORO);
    }
}

} // namespace novaio