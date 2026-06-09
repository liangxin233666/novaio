#include "novaio/runtime.hpp"
#include "novaio/scheduler.hpp"
#include "novaio/task.hpp"
#include "novaio/comutex.hpp"
#include "novaio/event.hpp"
#include "novaio/when_all.hpp"
#include "novaio/time_wheel.hpp"
#include "novaio/concurrent_queues.hpp"
#include "novaio/serializer.hpp"
#include "novaio/metrics.hpp"

#include <iostream>
#include <atomic>
#include <cassert>
#include <string>
#include <thread>
#include <chrono>
#include <stdexcept>
#include <vector>
#include <tuple>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

using namespace novaio;

// ==========================================
// 辅助工具与全局统计
// ==========================================
std::atomic<int> g_tests_passed{0};
std::atomic<int> g_tests_failed{0};

#define ASSERT_TEST(condition, test_name) \
    do { \
        if (condition) { \
            std::cout << "[✅ PASS] " << test_name << "\n"; \
            g_tests_passed++; \
        } else { \
            std::cerr << "[❌ FAIL] " << test_name << " (Line: " << __LINE__ << ")\n"; \
            g_tests_failed++; \
        } \
    } while(0)

// 帮助函数：调度分离任务
inline void dispatch_task(size_t core_id, auto task_gen) {
    Runtime::get().dispatch_on(core_id, UniqueTask([task_gen = std::move(task_gen)]() mutable {
        auto dt = task_gen();
        Scheduler::current()->schedule(dt.coro_);
    }));
}

// ==========================================
// 测试 1: 基础协程与异常传递 (Task<T> & Exception)
// ==========================================
Task<int> task_return_value() {
    co_await sleep_for(10);
    co_return 42;
}

Task<void> task_throw_exception() {
    co_await sleep_for(10);
    throw std::runtime_error("Expected Exception");
}

Task<void> test_coro_basics() {
    int val = co_await task_return_value();
    bool caught = false;
    try {
        co_await task_throw_exception();
    } catch (const std::runtime_error& e) {
        caught = std::string(e.what()) == "Expected Exception";
    }
    ASSERT_TEST(val == 42 && caught, "Task Return Value & Exception Propagation");
}

DetachedTask launch_test_coro_basics() { co_await test_coro_basics(); }

// ==========================================
// 测试 2: 并发结构与定时器 (when_all & TimeWheel)
// ==========================================
Task<int> delayed_val(int val, uint64_t ms) {
    co_await sleep_for(ms);
    co_return val;
}

Task<void> delayed_void(uint64_t ms) {
    co_await sleep_for(ms);
}

Task<void> test_when_all_and_timers() {
    auto start = Scheduler::current()->current_time_ms();

    // 混合不同类型的返回，测试 TimeWheel 不同层级级联 (300ms, 50ms, 150ms)
    auto [r1, r2, r3, r4] = co_await when_all(
        delayed_val(1, 300),
        delayed_val(2, 50),
        delayed_val(3, 150),
        delayed_void(100) // 修补：when_all 只能接收 Task<T>，使用包装函数
    );

    auto elapsed = Scheduler::current()->current_time_ms() - start;

    bool time_accurate = (elapsed >= 300 && elapsed <= 350);
    ASSERT_TEST(r1 == 1 && r2 == 2 && r3 == 3 && time_accurate, "when_all & TimeWheel Cascading");
}

DetachedTask launch_when_all_test() { co_await test_when_all_and_timers(); }

// ==========================================
// 测试 3: 跨线程同步 (CoMutex & Event)
// ==========================================
CoMutex g_stress_mutex;
int g_stress_counter = 0;
Event g_cross_event;
Event g_multi_waiter_event;

Task<void> mutex_stress_worker(int iterations) {
    for (int i = 0; i < iterations; i++) {
        co_await g_stress_mutex.lock();
        g_stress_counter++;
        g_stress_mutex.unlock();
    }
}

DetachedTask launch_mutex_worker(int iter) { co_await mutex_stress_worker(iter); }

Task<void> event_waiter_worker(std::atomic<bool>& flag) {
    co_await g_cross_event;
    flag = true;
}

DetachedTask launch_event_waiter(std::atomic<bool>& flag) { co_await event_waiter_worker(flag); }

Task<void> multi_event_waiter_worker(std::atomic<int>& resumed_count) {
    co_await g_multi_waiter_event;
    resumed_count.fetch_add(1, std::memory_order_relaxed);
}

DetachedTask launch_multi_event_waiter(std::atomic<int>& resumed_count) {
    co_await multi_event_waiter_worker(resumed_count);
}

// ==========================================
// 测试 4: 无锁并发队列 (SpscQueue)
// ==========================================
void test_spsc_queue() {
    SpscQueue<int> queue(1024);
    ASSERT_TEST(queue.push(100), "SpscQueue Push");
    ASSERT_TEST(queue.push(200), "SpscQueue Push");

    int val1, val2, val3;
    bool pop1 = queue.pop(val1);
    bool pop2 = queue.pop(val2);
    bool pop3 = queue.pop(val3); // 应该为空

    ASSERT_TEST(pop1 && val1 == 100 && pop2 && val2 == 200 && !pop3, "SpscQueue Pop Logic");
}

// ==========================================
// 测试 5: 序列化器 (Serializer)
// ==========================================
struct MyCustomData {
    int id;
    std::string name;
    double score;

    // 框架 serializer.hpp 要求的接口支持
    auto tie() const { return std::tie(id, name, score); }
};

void test_serializer() {
    MyCustomData original{42, "novaio", 3.14};
    std::vector<char> buffer;
    novaio::serialize(original, buffer);

    // 尺寸验证：sizeof(int) + sizeof(size_t) + 6("novaio".size()) + sizeof(double)
    size_t expected_size = sizeof(int) + sizeof(size_t) + 6 + sizeof(double);
    ASSERT_TEST(buffer.size() == expected_size, "Serializer Custom Struct/String/Arithmetic");
}

// ==========================================
// 测试 6: 网络 IO 引擎 (io_uring Direct FDs & Buffer Ring)
// ==========================================
std::atomic<bool> g_server_ready{false};
std::atomic<bool> g_io_test_passed{false};

Task<void> tcp_echo_server(int listen_fd) {
    auto* sched = Scheduler::current();
    g_server_ready = true;

    // 1. 测试 accept_direct
    auto accept_res = co_await sched->io_engine().accept_direct(listen_fd);
    int direct_client_fd = accept_res.res;
    if (direct_client_fd < 0) co_return;

    // 2. 测试 recv_direct (使用 Provided Buffer Ring)
    auto recv_res = co_await sched->io_engine().recv_direct(direct_client_fd);
    if (recv_res.res > 0) {
        // 从 flags 提取 buffer ID
        uint16_t bid = recv_res.flags >> IORING_CQE_BUFFER_SHIFT;
        void* buf = sched->io_engine().get_buffer(bid);

        // 3. 测试 send_direct
        auto send_res = co_await sched->io_engine().send_direct(direct_client_fd, buf, recv_res.res);

        // 验证收发数据一致性
        if (send_res.res == recv_res.res && std::string((char*)buf, recv_res.res) == "PING") {
            g_io_test_passed = true;
        }

        // 4. 归还 Buffer
        sched->io_engine().return_buffer(bid);
    }

    // 5. 测试 close_direct
    co_await sched->io_engine().close_direct(direct_client_fd);
}

DetachedTask launch_tcp_server(int fd) { co_await tcp_echo_server(fd); }

// ==========================================
// 主入口与测试编排
// ==========================================
int main() {
    std::cout << "======================================\n";
    std::cout << "🚀 NovaIO 终极全功能测试套件开始运行\n";
    std::cout << "======================================\n\n";

    // 独立跑非协程组件测试
    test_spsc_queue();
    test_serializer();

    // 启动运行时 (2 个核心)
    Runtime::get().start(2);

    //[测试群 1]: 基础协程与 when_all
    dispatch_task(0,[]{ return launch_test_coro_basics(); });
    dispatch_task(1,[]{ return launch_when_all_test(); });

    // [测试群 2]: 跨线程 Mutex 竞争测试
    const int MUTEX_ITERS = 5000;
    dispatch_task(0,[=]{ return launch_mutex_worker(MUTEX_ITERS); });
    dispatch_task(1, [=]{ return launch_mutex_worker(MUTEX_ITERS); });

    //[测试群 3]: 跨线程 Event 唤醒测试
    std::atomic<bool> event_triggered{false};
    std::atomic<int> multi_event_resumed{0};

    dispatch_task(0, [&]{ return launch_event_waiter(event_triggered); });
    dispatch_task(1, []{
        return[](auto) -> DetachedTask {
            co_await sleep_for(200); // 确保 Waiter 先进入等待
            g_cross_event.set();
        }(0); // 立即调用的 lambda 生成 DetachedTask
    });
    dispatch_task(0, [&]{ return launch_multi_event_waiter(multi_event_resumed); });
    dispatch_task(1, [&]{ return launch_multi_event_waiter(multi_event_resumed); });
    dispatch_task(0, []{
        return[](auto) -> DetachedTask {
            co_await sleep_for(250);
            g_multi_waiter_event.set();
        }(0);
    });

    //[测试群 4]: IO 引擎回环测试 (核心 0 跑服务器)
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(9999);
    bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(listen_fd, 10);

    dispatch_task(0,[listen_fd]{ return launch_tcp_server(listen_fd); });

    // 模拟一个外部客户端，向引擎发送请求以测试网络栈
    std::thread external_client([&]() {
        while (!g_server_ready) std::this_thread::sleep_for(std::chrono::milliseconds(10));

        int client_fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in serv_addr{};
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(9999);
        inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

        if (connect(client_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == 0) {
            const char* msg = "PING";
            send(client_fd, msg, 4, 0);

            char buffer[16] = {0};
            recv(client_fd, buffer, 16, 0);
        }
        close(client_fd);
    });

    // 等待所有异步测试完成
    std::cout << "⌛ 正在等待所有异步测试执行完毕 (预计 1-2 秒)...\n";
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 回收外部客户端线程
    external_client.join();
    close(listen_fd);

    // ==========================================
    // 断言结果汇总
    // ==========================================
    ASSERT_TEST(g_stress_counter == MUTEX_ITERS * 2, "CoMutex Cross-Thread High Contention");
    ASSERT_TEST(event_triggered.load(), "Event Cross-Thread Signaling");
    ASSERT_TEST(multi_event_resumed.load() == 2, "Event Multi-Waiter Broadcast");
    ASSERT_TEST(g_io_test_passed.load(), "IoEngine Direct FDs & Provided Buffer Ring (Echo Test)");

#ifdef NOVAIO_ENABLE_METRICS
    auto metrics = MetricsRegistry::get().aggregate();
    std::cout << "\n📊 框架内部指标:\n";
    std::cout << "   - 任务执行次数: " << metrics.tasks_executed << "\n";
    std::cout << "   - IO事件处理数: " << metrics.io_events_reaped << "\n";
    std::cout << "   - 跨核消息处理: " << metrics.inbox_messages_processed << "\n";
#else
    std::cout << "\n📊 框架内部指标: (未开启统计，若需查看请在 metrics.hpp 打开 NOVAIO_ENABLE_METRICS 宏)\n";
#endif

    std::cout << "\n======================================\n";
    if (g_tests_failed == 0) {
        std::cout << "🎉 完美！所有 " << g_tests_passed << " 项测试全部通过！框架表现稳定。\n";
    } else {
        std::cout << "⚠️ 警告！有 " << g_tests_failed << " 项测试未通过。请检查日志。\n";
    }
    std::cout << "======================================\n";

    Runtime::get().stop();
    return g_tests_failed == 0 ? 0 : 1;
}
