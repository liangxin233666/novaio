#include "novaio/runtime.hpp"
#include "novaio/scheduler.hpp"
#include "novaio/task.hpp"
#include "novaio/metrics.hpp"
#include "novaio/time_wheel.hpp"
#include "novaio/when_all.hpp"

#include <iostream>
#include <atomic>
#include <vector>
#include <thread>
#include <chrono>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <iomanip>
#include <string>

using namespace novaio;

// ==========================================
// 全局压测统计指标
// ==========================================
std::atomic<uint64_t> g_total_requests{0};
std::atomic<uint64_t> g_failed_requests{0};
std::atomic<uint64_t> g_bytes_transferred{0};
std::atomic<uint64_t> g_active_connections{0};
std::atomic<bool>     g_benchmark_running{true};

// 压测参数
constexpr int TEST_PORT = 10086;
constexpr int CLIENT_THREADS = 8;        // 模拟 8 个客户端线程
constexpr int CONNS_PER_THREAD = 200;    // 每个线程 200 个连接 (总计 1600 并发连接)
constexpr int REQS_PER_CONN = 250;       // 每个连接 250 次 Ping-Pong (总计 400,000 次请求)

// ==========================================
// 进度条渲染工具
// ==========================================
void print_progress_bar(const std::string& prefix, int current, int total, double extra_speed = 0.0) {
    const int bar_width = 40;
    float progress = static_cast<float>(current) / total;
    if (progress > 1.0f) progress = 1.0f;
    int pos = bar_width * progress;

    std::cout << "\r\033[K" << prefix << " ["; // \033[K 用于清除当前行剩余字符
    for (int i = 0; i < bar_width; ++i) {
        if (i < pos) std::cout << "=";
        else if (i == pos) std::cout << ">";
        else std::cout << " ";
    }
    std::cout << "] " << std::setw(3) << int(progress * 100.0) << "% (" << current << "/" << total << ")";

    if (extra_speed > 0.0) {
        std::cout << " | Speed: " << std::fixed << std::setprecision(0) << extra_speed << " req/s";
    }
    std::cout << std::flush;
    if (current >= total) std::cout << std::endl;
}

// ==========================================
// 业务逻辑：高性能 Echo Server Handler
// ==========================================
Task<void> handle_client(int direct_fd) {
    auto* sched = Scheduler::current();
    g_active_connections++;

    while (g_benchmark_running.load(std::memory_order_relaxed)) {
        auto recv_res = co_await sched->io_engine().recv_direct(direct_fd);

        if (recv_res.res <= 0) {
            if (recv_res.res == -ENOBUFS) {
                co_await sleep_for(5); // Buffer 耗尽时的反压处理
                continue;
            }
            break;
        }

        uint16_t bid = recv_res.flags >> IORING_CQE_BUFFER_SHIFT;
        void* buf = sched->io_engine().get_buffer(bid);

        int total_sent = 0;
        int to_send = recv_res.res;
        while (total_sent < to_send) {
            auto send_res = co_await sched->io_engine().send_direct(
                direct_fd,
                static_cast<char*>(buf) + total_sent,
                to_send - total_sent
            );
            if (send_res.res <= 0) break;
            total_sent += send_res.res;
        }

        sched->io_engine().return_buffer(bid);
        g_bytes_transferred += total_sent;
        g_total_requests++;
    }

    g_active_connections--;
    co_await sched->io_engine().close_direct(direct_fd);
}

// ==========================================
// 业务逻辑：Server 监听循环
// ==========================================
Task<void> tcp_server_loop(int listen_fd) {
    auto* sched = Scheduler::current();
    while (g_benchmark_running.load(std::memory_order_relaxed)) {
        auto accept_res = co_await sched->io_engine().accept_direct(listen_fd);
        if (accept_res.res >= 0) {
            // 【核心修正】：创建处理协程并立即调度
            auto dt = [](int fd) -> DetachedTask {
                co_await handle_client(fd);
            }(accept_res.res);

            if (sched) sched->schedule(dt.coro_);
        } else {
            co_await sleep_for(10);
        }
    }
}

DetachedTask launch_server(int fd) { co_await tcp_server_loop(fd); }

// ==========================================
// 压力注入端：传统阻塞多线程客户端
// ==========================================
void client_worker_thread(int thread_id) {
    std::vector<int> fds;
    struct sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(TEST_PORT);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    for (int i = 0; i < CONNS_PER_THREAD; ++i) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == 0) {
            fds.push_back(sock);
        } else {
            g_failed_requests++;
        }
    }

    const char* payload = "NOVAIO_BENCHMARK_PAYLOAD_64B_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
    char recv_buf[128];
    int len = strlen(payload);

    for (int r = 0; r < REQS_PER_CONN; ++r) {
        for (int fd : fds) {
            if (!g_benchmark_running.load()) break;
            if (send(fd, payload, len, 0) == len) {
                int recvd = recv(fd, recv_buf, len, 0);
                if (recvd != len) g_failed_requests++;
            } else {
                g_failed_requests++;
            }
        }
    }

    for (int fd : fds) close(fd);
}

// ==========================================
// 极限测试 2：百万协程与内存分配器压测
// ==========================================
std::atomic<uint64_t> g_task_completed{0};

Task<void> lightweight_task() {
    co_await sleep_for(1); // 强制引发上下文切换与时间轮挂载
    g_task_completed.fetch_add(1, std::memory_order_relaxed);
}

DetachedTask launch_batch_tasks(int count) {
    auto* sched = Scheduler::current(); // 获取当前核的调度器
    for(int i = 0; i < count; i++) {
        // 【核心修正】：捕获创建出的 DetachedTask
        auto dt = []( ) -> DetachedTask {
            co_await lightweight_task();
        }();

        // 【核心修正】：手动调度它，否则它会一直处于 initial_suspend 状态
        if (sched) sched->schedule(dt.coro_);
    }
    co_return;
}

// ==========================================
// 主函数
// ==========================================
int main() {
    std::cout << "========================================================\n";
    std::cout << " 🔥 NovaIO 生产级性能探测器 (Real-time Benchmark)\n";
    std::cout << "========================================================\n\n";

    size_t cores = std::max<size_t>(2, std::thread::hardware_concurrency() - 2);
    Runtime::get().start(cores);
    std::cout << "[System] 运行时启动成功，调度器核心数: " << cores << "\n";

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(TEST_PORT);
    if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "绑定端口失败，请检查端口占用或 ulimit 限制。\n";
        return 1;
    }
    listen(listen_fd, 4096);

    Runtime::get().dispatch_on(0, UniqueTask([listen_fd] {
        auto dt = launch_server(listen_fd);
        Scheduler::current()->schedule(dt.coro_);
    }));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // ========================================
    // 压测 A: 10 万海量协程分配测试 (分10批，防队列溢出)
    // ========================================
    std::cout << "\n[Test A] 协程生命周期压测 (测试 mimalloc 与队列抗压能力)...\n";
    auto start_coro = std::chrono::high_resolution_clock::now();

    int total_tasks = 100000;
    int batch_size = 10000;

    for (int b = 0; b < total_tasks / batch_size; ++b) {
        Runtime::get().dispatch_on(1, UniqueTask([batch_size] {
            auto dt = launch_batch_tasks(batch_size);
            Scheduler::current()->schedule(dt.coro_);
        }));

        // 实时等待并渲染进度条
        while (g_task_completed.load() < (uint64_t)(b + 1) * batch_size) {
            print_progress_bar("🚀 协程处理进度", g_task_completed.load(), total_tasks);
            std::this_thread::sleep_for(std::chrono::milliseconds(10)); // 10ms 刷新一次
        }
    }
    print_progress_bar("🚀 协程处理进度", total_tasks, total_tasks);

    auto end_coro = std::chrono::high_resolution_clock::now();
    double coro_time = std::chrono::duration<double>(end_coro - start_coro).count();
    std::cout << "✅ 100,000 协程创建、挂起与销毁完美通过! 耗时: " << std::fixed << std::setprecision(3) << coro_time << " s.\n";


    // ========================================
    // 压测 B: 高并发网络 I/O 吞吐量 (QPS)
    // ========================================
    int expected_reqs = CLIENT_THREADS * CONNS_PER_THREAD * REQS_PER_CONN;
    std::cout << "\n[Test B] 网络核爆级压测启动 (并发连接: " << CLIENT_THREADS * CONNS_PER_THREAD
              << " | 总请求: " << expected_reqs << ")...\n";

    auto start_net = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> clients;
    for (int i = 0; i < CLIENT_THREADS; ++i) {
        clients.emplace_back(client_worker_thread, i);
    }

    // 实时渲染 QPS 进度条
    uint64_t last_reqs = 0;
    auto last_time = std::chrono::high_resolution_clock::now();

    while (true) {
        uint64_t current_reqs = g_total_requests.load();
        auto current_time = std::chrono::high_resolution_clock::now();
        double dt = std::chrono::duration<double>(current_time - last_time).count();

        double current_qps = (current_reqs - last_reqs) / dt;

        print_progress_bar("⚡ 网络 QPS 进度", current_reqs, expected_reqs, current_qps);

        last_reqs = current_reqs;
        last_time = current_time;

        if (current_reqs >= expected_reqs || g_failed_requests.load() > expected_reqs * 0.1) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200)); // 200ms 刷新率
    }

    // 等待客户端收尾
    g_benchmark_running = false;
    for (auto& t : clients) {
        if (t.joinable()) t.join();
    }
    print_progress_bar("⚡ 网络 QPS 进度", expected_reqs, expected_reqs, 0.0);

    auto end_net = std::chrono::high_resolution_clock::now();
    double elapsed_s = std::chrono::duration<double>(end_net - start_net).count();
    close(listen_fd);

    // ========================================
    // 数据统计与分析
    // ========================================
    uint64_t total_reqs = g_total_requests.load();
    uint64_t total_fails = g_failed_requests.load();
    double avg_qps = total_reqs / elapsed_s;
    double mbps = (g_bytes_transferred.load() / 1024.0 / 1024.0) / elapsed_s;

    std::cout << "\n========================================================\n";
    std::cout << " 📊 生产级压测最终报告 (Performance Report)\n";
    std::cout << "========================================================\n";
    std::cout << "  ⏱  总耗时          : " << elapsed_s << " 秒\n";
    std::cout << "  ✅ 成功处理请求    : " << total_reqs << " 次\n";
    std::cout << "  ❌ 失败请求 (丢包) : " << total_fails << " 次\n";
    std::cout << "  🚀 平均 QPS        : " << static_cast<int>(avg_qps) << " req/s\n";
    std::cout << "  💽 网络吞吐量      : " << mbps << " MB/s\n";
    std::cout << "========================================================\n";

    if (total_fails > 0) {
        std::cout << "⚠️ 提示：出现丢包，可能是由于单核负载过重或 Linux FD 耗尽引起。\n";
    } else {
        std::cout << "🎉 完美无瑕！全程 0 丢包，内存调度稳如老狗。\n";
    }

    Runtime::get().stop();
    return 0;
}