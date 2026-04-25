#pragma once

#include <liburing.h>
#include <coroutine>
#include <cstdint>
#include <mimalloc.h>
#include <system_error>

namespace novaio {

constexpr uint32_t IO_QUEUE_DEPTH = 4096;
constexpr uint16_t BUF_BGID = 1;
constexpr uint32_t BUF_RING_SIZE = 2048;
constexpr uint32_t BUF_BLOCK_SIZE = 2048;
constexpr uint32_t MAX_DIRECT_FDS = 65536;

// UserData 标记位：最低位为1表示多射模式
constexpr uint64_t MULTISHOT_FLAG = 1ULL; 

struct IoResult {
    int res;
    uint32_t flags;
};

// 单射等待者
struct IoAwaiter {
    struct io_uring* ring_;
    struct io_uring_sqe* sqe_; 
    int res_ = 0;
    uint32_t flags_ = 0;
    std::coroutine_handle<> coro_{nullptr};
    
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept {
        coro_ = h;
        if (sqe_) {
            io_uring_sqe_set_data(sqe_, this); 
        }
    }
    IoResult await_resume() const noexcept { return {res_, flags_}; }
};

// 多射函数指针
using MultishotCallback = void(*)(int res, uint32_t flags);

class IoEngine {
public:
    IoEngine() = default;
    ~IoEngine();

    void init(); 
    void poll();

    // 单射
    IoAwaiter accept_direct(int server_fd);
    IoAwaiter recv_direct(int direct_fd);
    IoAwaiter send_direct(int direct_fd, const void* buf, size_t len);
    IoAwaiter close_direct(int direct_fd);

    // 多射
    void accept_multishot(int server_fd, MultishotCallback cb);

    int ring_fd() const noexcept { return ring_.ring_fd; }
    void* get_buffer(uint16_t buffer_id) const noexcept;
    void return_buffer(uint16_t buffer_id) noexcept;
    struct io_uring* raw_ring() noexcept { return &ring_; }

private:
    void setup_provided_buffer_ring();
    bool initialized_{false}; 
    struct io_uring ring_;
    struct io_uring_buf_ring* br_{nullptr};
    void* buffer_base_{nullptr};
};

} // namespace novaio