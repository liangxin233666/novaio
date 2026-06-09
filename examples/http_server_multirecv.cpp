#include "novaio/runtime.hpp"
#include "novaio/task.hpp"
#include "novaio/scheduler.hpp"
#include "novaio/io_engine.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <print>
#include <string_view>
#include <vector>

using namespace novaio;


constexpr std::string_view HTTP_RESPONSE =
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: 13\r\n"
    "Connection: keep-alive\r\n"
    "\r\n"
    "Hello NovaIO!";

constexpr uint32_t CQE_F_BUFFER = 1U << 0;

DetachedTask handle_business_logic(int direct_fd, int send_count) {
    auto* scheduler = Scheduler::current();
    if (!scheduler) co_return;

    for (int i = 0; i < send_count; ++i) {
        auto result = co_await scheduler->io_engine().send_direct(
            direct_fd, HTTP_RESPONSE.data(), HTTP_RESPONSE.size());

        if (result.res <= 0) {
            break;
        }
    }
}


struct Connection : public MultishotOp {
    int fd{-1};
    int counter{0};
};


alignas(64) Connection g_connections[65536];


static void on_client_recv(MultishotOp* self, int res, uint32_t flags) {
    auto* conn = static_cast<Connection*>(self);
    auto* scheduler = Scheduler::current();


    if (flags & CQE_F_BUFFER) {
        uint16_t bid = flags >> 16;
        scheduler->io_engine().return_buffer(bid);
    }


    if (res <= 0) {
        scheduler->schedule([](int fd) -> DetachedTask {
            co_await Scheduler::current()->io_engine().close_direct(fd);
        }(conn->fd).coro_);
        return;
    }

    conn->counter++;

    scheduler->schedule(handle_business_logic(conn->fd, conn->counter).coro_);

    if (!(flags & IORING_CQE_F_MORE)) {
        scheduler->io_engine().recv_multishot(conn->fd, conn);
    }
}


struct Acceptor : public MultishotOp {};
Acceptor g_acceptor;

static void on_client_accept(MultishotOp* self, int res, uint32_t flags) {
    if (res >= 0) {
        int direct_fd = res;

        auto& conn = g_connections[direct_fd];
        conn.fd = direct_fd;
        conn.counter = 0;
        conn.invoke = on_client_recv;

        Scheduler::current()->io_engine().recv_multishot(direct_fd, &conn);
    }
}

void start_server(int port) {
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
    listen(server_fd, 65535);

    g_acceptor.invoke = on_client_accept;
    Scheduler::current()->io_engine().accept_multishot(server_fd, &g_acceptor);
}

int main() {

    size_t cores = std::thread::hardware_concurrency();
    Runtime::get().start(cores);

    for (size_t i = 0; i < 8; ++i) {
        Runtime::get().dispatch_on(i, UniqueTask([]() {

            start_server(8080);
        }));
    }

    std::println("NovaIO Framework [2026.04.25 Edition] is running.");
    std::println("Mode: Multishot-Accept + Multishot-Recv + Coroutine Business Logic.");

    while(true) {
        std::this_thread::sleep_for(std::chrono::hours(24));
    }

    return 0;
}