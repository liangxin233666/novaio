#pragma once

#include <coroutine>
#include <cstdint>
#include <array>

namespace novaio {

struct TimerNode {
    uint64_t expires_at;
    std::coroutine_handle<> handle;
    TimerNode* next{nullptr};
    TimerNode* prev{nullptr};
    TimerNode** head_ptr{nullptr}; 
};

class TimeWheel {
public:
    TimeWheel() = default;

    void add_timer(TimerNode* node);
    void remove_timer(TimerNode* node);
    void tick(uint64_t current_time);

    uint64_t next_timeout(uint64_t current_time) const;
    uint64_t active_timers() const noexcept { return active_timers_; }

private:
    static constexpr uint32_t TVR_BITS = 8;
    static constexpr uint32_t TVN_BITS = 6;
    static constexpr uint32_t TVR_SIZE = 1 << TVR_BITS;
    static constexpr uint32_t TVN_SIZE = 1 << TVN_BITS;
    static constexpr uint32_t TVR_MASK = TVR_SIZE - 1;
    static constexpr uint32_t TVN_MASK = TVN_SIZE - 1;

    std::array<TimerNode*, TVR_SIZE> tv1_{};
    std::array<TimerNode*, TVN_SIZE> tv2_{};
    std::array<TimerNode*, TVN_SIZE> tv3_{};
    std::array<TimerNode*, TVN_SIZE> tv4_{};
    std::array<TimerNode*, TVN_SIZE> tv5_{};

    uint64_t current_tick_{0};
    uint64_t active_timers_{0};
    void cascade(std::array<TimerNode*, TVN_SIZE>& tv, uint32_t index);
};

struct SleepAwaiter {
    uint64_t ms_;
    TimerNode node_;

    explicit SleepAwaiter(uint64_t ms) noexcept : ms_(ms) {}

    bool await_ready() const noexcept { return ms_ == 0; }
    void await_suspend(std::coroutine_handle<> h) noexcept;
    void await_resume() const noexcept {}
};

inline SleepAwaiter sleep_for(uint64_t ms) noexcept {
    return SleepAwaiter{ms};
}

} // namespace novaio