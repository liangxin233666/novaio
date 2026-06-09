#include "novaio/runtime.hpp"
#include "novaio/task.hpp"
#include "novaio/scheduler.hpp"
#include "novaio/io_engine.hpp"
#include "novaio/time_wheel.hpp"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <print>
#include <string_view>
#include <thread>
#include <chrono>
#include <algorithm>
#include <cstdlib>
#include <cerrno>

using namespace novaio;


constexpr std::string_view HTTP_RESPONSE =
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: 13\r\n"
    "Connection: keep-alive\r\n"
    "\r\n"
    "Hello NovaIO!";

constexpr uint32_t CQE_F_BUFFER = 1U << 0;

size_t env_size_or(const char* name, size_t fallback) {
    if (const char* value = std::getenv(name)) {
        char* end = nullptr;
        unsigned long parsed = std::strtoul(value, &end, 10);
        if (end != value && parsed > 0) {
            return static_cast<size_t>(parsed);
        }
    }
    return fallback;
}


DetachedTask handle_client(int direct_fd) {
    auto* scheduler = Scheduler::current();
    if (!scheduler) co_return;

    while (true) {

        auto result = co_await scheduler->io_engine().recv_direct(direct_fd);

        if (result.flags & CQE_F_BUFFER) {
            uint16_t buf_id = result.flags >> 16;

            scheduler->io_engine().return_buffer(buf_id);
        }


        if (result.res <= 0) break;


        auto send_result = co_await scheduler->io_engine().send_direct(
            direct_fd, HTTP_RESPONSE.data(), HTTP_RESPONSE.size());

        if (send_result.res < 0) break;
    }


    co_await scheduler->io_engine().close_direct(direct_fd);
}


struct Acceptor : public MultishotOp {};

static void on_accept_callback(MultishotOp*, int res, uint32_t flags) {
    if (res >= 0) {

        Scheduler::current()->schedule(handle_client(res).coro_);
    } else {
        if (res != -EAGAIN && res != -EINTR) {
            std::println(stderr, "Multishot Accept error: {}", res);
        }
    }
}


void start_multishot_server(int port) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1, defer = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    setsockopt(server_fd, IPPROTO_TCP, TCP_DEFER_ACCEPT, &defer, sizeof(defer));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return;
    }

    listen(server_fd, 32768);

    std::println("NovaIO listening on {}...", port);
    std::println("Status: Multishot Accept enabled, UserData Pointer Tagging active.");

    auto* scheduler = Scheduler::current();
    auto* acceptor = new Acceptor{};
    acceptor->invoke = on_accept_callback;
    scheduler->io_engine().accept_multishot(server_fd, acceptor);
}

int main() {
    size_t hardware = std::max<size_t>(1, std::thread::hardware_concurrency());
    size_t cores = env_size_or("NOVAIO_CORES", hardware);
    size_t listeners = env_size_or("NOVAIO_LISTENERS", cores);
    listeners = std::min(listeners, cores);

    std::println("NovaIO runtime cores: {}, listener schedulers: {}", cores, listeners);
    Runtime::get().start(cores);


    for (size_t i = 0; i < listeners; ++i) {
        Runtime::get().dispatch_on(i, UniqueTask([i]() {
            start_multishot_server(8080);
        }));
    }


    while(true) {
        std::this_thread::sleep_for(std::chrono::hours(24));
    }

    return 0;
}
