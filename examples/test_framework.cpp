#include "novaio/runtime.hpp"
#include "novaio/scheduler.hpp"
#include "novaio/task.hpp"
#include "novaio/comutex.hpp"
#include "novaio/event.hpp"
#include "novaio/when_all.hpp"
#include "novaio/time_wheel.hpp"
#include <iostream>
#include <atomic>
#include <cassert>
#include <string>
#include <thread>
#include <chrono>

using namespace novaio;

CoMutex g_mutex;
int g_counter = 0;
Event g_event;
std::atomic<int> g_tests_passed{0};

Task<void> mutex_worker(int id) {
    for (int i = 0; i < 1000; i++) {
        // 【已修复】：去掉了 auto lock = ，因为 await_resume 返回的是 void
        co_await g_mutex.lock();
        g_counter++;
        g_mutex.unlock();
    }
    std::cout << "[Test 1] CoMutex Worker " << id << " finished.\n";
}

DetachedTask launch_mutex_worker(int id) { co_await mutex_worker(id); }


Task<void> event_waiter() {
    std::cout << "[Test 2] Event Waiter (Core 0) waiting...\n";
    co_await g_event;
    std::cout << "[Test 2] Event Waiter (Core 0) resumed by Core 1!\n";
    g_tests_passed++;
}

Task<void> event_setter() {
    co_await sleep_for(100);
    std::cout << "[Test 2] Event Setter (Core 1) firing event...\n";
    g_event.set();
}

DetachedTask launch_event_waiter() { co_await event_waiter(); }
DetachedTask launch_event_setter() { co_await event_setter(); }


Task<int> async_add(int a, int b) {
    co_await sleep_for(50);
    co_return a + b;
}

Task<std::string> async_string() {
    co_await sleep_for(30);
    co_return "Hello NovaIO";
}

Task<void> async_void() {
    co_await sleep_for(10);
    co_return;
}

Task<void> test_when_all_task() {
    std::cout << "[Test 3] Testing when_all launching 3 concurrent tasks...\n";

    auto [res1, res2, dummy] = co_await when_all(
        async_add(10, 20),
        async_string(),
        async_void()
    );

    if (res1 == 30 && res2 == "Hello NovaIO") {
        std::cout << "[Test 3] when_all passed successfully!\n";
        g_tests_passed++;
    }
}

DetachedTask launch_test_when_all() { co_await test_when_all_task(); }


int main() {
    std::cout << "Starting NovaIO Framework Tests...\n";


    Runtime::get().start(2);

    Runtime::get().dispatch_on(0, UniqueTask([]{
        auto dt = launch_test_when_all();
        Scheduler::current()->schedule(dt.coro_);
    }));


    Runtime::get().dispatch_on(0, UniqueTask([]{
        auto dt = launch_mutex_worker(1);
        Scheduler::current()->schedule(dt.coro_);
    }));
    Runtime::get().dispatch_on(1, UniqueTask([]{
        auto dt = launch_mutex_worker(2);
        Scheduler::current()->schedule(dt.coro_);
    }));

    Runtime::get().dispatch_on(0, UniqueTask([]{
        auto dt = launch_event_waiter();
        Scheduler::current()->schedule(dt.coro_);
    }));
    Runtime::get().dispatch_on(1, UniqueTask([]{
        auto dt = launch_event_setter();
        Scheduler::current()->schedule(dt.coro_);
    }));

    std::cout << "Tests are running, waiting for results...\n";
    std::this_thread::sleep_for(std::chrono::seconds(2));

    std::cout << "\n--- Final Results ---\n";

    if (g_counter == 2000) {
        std::cout << "[SUCCESS] Test 1: CoMutex FIFO/CAS competition passed (Counter: " << g_counter << ")\n";
    } else {
        std::cerr << "[FAILED] Test 1: CoMutex competition failed! Counter: " << g_counter << "\n";
    }

    if (g_tests_passed.load() == 2) {
        std::cout << "[SUCCESS] Test 2 & 3: Event signaling and structured when_all passed.\n";
    } else {
        std::cerr << "[FAILED] Test 2 or 3 failed! Passed count: " << g_tests_passed.load() << "\n";
    }

    std::cout << "Shutting down runtime...\n";
    Runtime::get().stop();
    return 0;
}