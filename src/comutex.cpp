#include "novaio/comutex.hpp"
#include "novaio/scheduler.hpp"
#include "novaio/runtime.hpp"

namespace novaio {

bool CoMutex::Awaiter::await_suspend(std::coroutine_handle<> h) noexcept {
    awaiting_coroutine_ = h;
    scheduler_ = Scheduler::current(); 

    uintptr_t current = mutex_.state_.load(std::memory_order_relaxed);
    
    while (true) {
        if (current == UNLOCKED) {
            if (mutex_.state_.compare_exchange_weak(
                    current, LOCKED_NO_WAITERS,
                    std::memory_order_acquire,
                    std::memory_order_relaxed)) {
                return false; 
            }
        } else {
            next_ = reinterpret_cast<Awaiter*>(current == LOCKED_NO_WAITERS? 0 : current);
            if (mutex_.state_.compare_exchange_weak(
                    current, reinterpret_cast<uintptr_t>(this),
                    std::memory_order_release,
                    std::memory_order_relaxed)) {
                return true; 
            }
        }
    }
}

void CoMutex::unlock() noexcept {
    uintptr_t current = state_.load(std::memory_order_relaxed);
    
    while (true) {
        if (current == LOCKED_NO_WAITERS) {
            if (state_.compare_exchange_weak(
                    current, UNLOCKED,
                    std::memory_order_release,
                    std::memory_order_relaxed)) {
                return; 
            }
        } else {
            auto* head = reinterpret_cast<Awaiter*>(current);
            auto* next = head->next_;
            uintptr_t new_state = next? reinterpret_cast<uintptr_t>(next) : LOCKED_NO_WAITERS;
            
            if (state_.compare_exchange_weak(
                    current, new_state,
                    std::memory_order_release,
                    std::memory_order_relaxed)) {
                
               
                auto* current_sched = Scheduler::current();
                auto* target_sched = head->scheduler_;
                
                if (target_sched == current_sched && current_sched != nullptr) {
                 
                    current_sched->schedule(head->awaiting_coroutine_);
                } else if (target_sched != nullptr) {
                  
                    Runtime::get().dispatch_on(target_sched->id(), [coro = head->awaiting_coroutine_, target_sched]() {
                        target_sched->schedule(coro);
                    });
                }
                return;
            }
        }
    }
}

} // namespace novaio