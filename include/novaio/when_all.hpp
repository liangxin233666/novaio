#pragma once
#include <coroutine>
#include <atomic>
#include <tuple>
#include <utility>
#include <type_traits>
#include "novaio/scheduler.hpp"
#include "novaio/task.hpp"

namespace novaio {

namespace detail {
    struct WhenAllCounter {
        std::atomic<size_t> count_;
        std::coroutine_handle<> continuation_{nullptr};

        explicit WhenAllCounter(size_t c) noexcept : count_(c) {}

        void notify() noexcept {
            if (count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                if (continuation_) {
                    auto* sched = Scheduler::current();
                    if (sched) sched->schedule(continuation_);
                    else continuation_.resume(); // Fallback
                }
            }
        }
    };

    struct WhenAllHelper {
        struct promise_type {
            WhenAllHelper get_return_object() { return {}; }
            std::suspend_never initial_suspend() noexcept { return {}; }
            std::suspend_never final_suspend() noexcept { return {}; }
            void return_void() noexcept {}
            void unhandled_exception() noexcept { std::terminate(); }
        };
    };


    template <typename T, typename R>
    WhenAllHelper run_task(Task<T>& task, R& result, WhenAllCounter& counter) {
        if constexpr (std::is_same_v<T, void>) {
            co_await task;
            result = 0;
        } else {
            result = co_await task;
        }
        counter.notify();
    }
}


template<typename T> struct result_type_mapper { using type = T; };
template<> struct result_type_mapper<void> { using type = int; };
template<typename T> using result_t = typename result_type_mapper<T>::type;

template <typename... T>
class WhenAllAwaiter {
public:
    explicit WhenAllAwaiter(Task<T>&&... tasks)
        : tasks_(std::move(tasks)...), counter_(sizeof...(T)) {}

    bool await_ready() const noexcept { return sizeof...(T) == 0; }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        if constexpr (sizeof...(T) == 0) {
            h.resume();
            return;
        }
        counter_.continuation_ = h;
        start_tasks(std::make_index_sequence<sizeof...(T)>{});
    }

    std::tuple<result_t<T>...> await_resume() {
        return std::move(results_);
    }

private:
    template <std::size_t... Is>
    void start_tasks(std::index_sequence<Is...>) {
        (detail::run_task(std::get<Is>(tasks_), std::get<Is>(results_), counter_), ...);
    }

    std::tuple<Task<T>...> tasks_;
    std::tuple<result_t<T>...> results_;
    detail::WhenAllCounter counter_;
};


template <typename... T>
auto when_all(Task<T>&&... tasks) {
    return WhenAllAwaiter<T...>(std::move(tasks)...);
}

} // namespace novaio