#include "novaio/time_wheel.hpp"
#include "novaio/scheduler.hpp"

namespace novaio {

void TimeWheel::add_timer(TimerNode* node) {
    active_timers_++;
    uint64_t expires = node->expires_at;
    uint64_t idx = expires - current_tick_;
    TimerNode** head = nullptr;

    if (idx < TVR_SIZE) {
        head = &tv1_[expires & TVR_MASK];
    } else if (idx < (1ULL << (TVR_BITS + TVN_BITS))) {
        head = &tv2_[(expires >> TVR_BITS) & TVN_MASK];
    } else if (idx < (1ULL << (TVR_BITS + 2 * TVN_BITS))) {
        head = &tv3_[(expires >> (TVR_BITS + TVN_BITS)) & TVN_MASK];
    } else if (idx < (1ULL << (TVR_BITS + 3 * TVN_BITS))) {
        head = &tv4_[(expires >> (TVR_BITS + 2 * TVN_BITS)) & TVN_MASK];
    } else {
        head = &tv5_[(expires >> (TVR_BITS + 3 * TVN_BITS)) & TVN_MASK];
    }

    node->next = *head;
    if (*head) (*head)->prev = node;
    node->prev = nullptr;
    node->head_ptr = head; 
    *head = node;
}

void TimeWheel::remove_timer(TimerNode* node) {
    if (node->head_ptr) active_timers_--;
    if (node->prev) {
        node->prev->next = node->next;
    } else if (node->head_ptr && *(node->head_ptr) == node) {
        *(node->head_ptr) = node->next;
    }
    
    if (node->next) {
        node->next->prev = node->prev;
    }

    node->head_ptr = nullptr;
    node->prev = nullptr;
    node->next = nullptr;
}

void TimeWheel::cascade(std::array<TimerNode*, TVN_SIZE>& tv, uint32_t index) {
    TimerNode* curr = tv[index];
    tv[index] = nullptr;
    while (curr) {
        TimerNode* next = curr->next;
        add_timer(curr);
        curr = next;
    }
}

void TimeWheel::tick(uint64_t current_time) {
    while (current_tick_ < current_time) {
        uint32_t index = current_tick_ & TVR_MASK;
        
        if (!index && current_tick_ > 0) {
            uint32_t i2 = (current_tick_ >> TVR_BITS) & TVN_MASK;
            cascade(tv2_, i2);
            if (!i2) {
                uint32_t i3 = (current_tick_ >> (TVR_BITS + TVN_BITS)) & TVN_MASK;
                cascade(tv3_, i3);
                if (!i3) {
                    uint32_t i4 = (current_tick_ >> (TVR_BITS + 2 * TVN_BITS)) & TVN_MASK;
                    cascade(tv4_, i4);
                    if (!i4) {
                        uint32_t i5 = (current_tick_ >> (TVR_BITS + 3 * TVN_BITS)) & TVN_MASK;
                        cascade(tv5_, i5);
                    }
                }
            }
        }

        TimerNode* curr = tv1_[index];
        tv1_[index] = nullptr;
        while (curr) {
            TimerNode* next = curr->next;
            curr->head_ptr = nullptr; 
            
            if (Scheduler::current()) {
                Scheduler::current()->schedule(curr->handle);
            }
            curr = next;
        }
        current_tick_++;
    }
}

uint64_t TimeWheel::next_timeout(uint64_t current_time) const {
    if (active_timers_ == 0) return (uint64_t)-1; 
    
    for (uint32_t i = 0; i < TVR_SIZE; ++i) {
        uint32_t index = (current_tick_ + i) & TVR_MASK;
        if (tv1_[index]) {
            uint64_t expires = tv1_[index]->expires_at;
            return (expires > current_time) ? (expires - current_time) : 0;
        }
    }
    return 50; 
}

void SleepAwaiter::await_suspend(std::coroutine_handle<> h) noexcept {
    auto scheduler = Scheduler::current();
    if (scheduler) {
        node_.expires_at = scheduler->current_time_ms() + ms_;
        node_.handle = h;
        scheduler->time_wheel().add_timer(&node_);
    }
}

} // namespace novaio