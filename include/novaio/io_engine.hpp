#pragma once

#include <liburing.h>
#include <coroutine>
#include <cstdint>
#include <system_error>
#include "novaio/io_context.hpp"

namespace novaio {

constexpr uint32_t IO_QUEUE_DEPTH = 4096;
constexpr uint16_t BUF_BGID = 1;
constexpr uint32_t BUF_RING_SIZE = 2048;
constexpr uint32_t BUF_BLOCK_SIZE = 2048;
constexpr uint32_t MAX_DIRECT_FDS = 65536;

constexpr uintptr_t TAG_MASK  = 0b111;
constexpr uintptr_t TAG_IO    = 0b000;
constexpr uintptr_t TAG_MULTI = 0b001;
constexpr uintptr_t TAG_CORO  = 0b010;
constexpr uintptr_t TAG_INBOX = 0b011;

struct MultishotOp {
    void (*invoke)(MultishotOp* self, int res, uint32_t flags) = nullptr;
};

class IoEngine {
public:
    IoEngine() = default;
    ~IoEngine();

    void init();
    void poll();

    SafeIoAwaiter accept_direct(int server_fd);
    SafeIoAwaiter recv_direct(int direct_fd);
    SafeIoAwaiter send_direct(int direct_fd, const void* buf, size_t len);
    SafeIoAwaiter close_direct(int direct_fd);

    void accept_multishot(int server_fd, MultishotOp* op);
    void recv_multishot(int direct_fd, MultishotOp* op);

    int ring_fd() const noexcept { return initialized_ ? ring_.ring_fd : -1; }
    void* get_buffer(uint16_t buffer_id) const noexcept;
    void return_buffer(uint16_t buffer_id) noexcept;
    struct io_uring* raw_ring() noexcept { return &ring_; }
    void send_msg_ring(int target_ring_fd, uint64_t data);

private:
    void setup_provided_buffer_ring();
    bool initialized_{false};
    struct io_uring ring_;
    struct io_uring_buf_ring* br_{nullptr};
    void* buffer_base_{nullptr};
};

} // namespace novaio