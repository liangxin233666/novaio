#include "novaio/io_engine.hpp"
#include <cstring>
#include <stdexcept>
#include <sys/socket.h>
#include "novaio/scheduler.hpp"
#include "novaio/metrics.hpp"

namespace novaio {

void IoEngine::init() {
    constexpr unsigned advanced_flags = IORING_SETUP_SINGLE_ISSUER |
                                        IORING_SETUP_DEFER_TASKRUN |
                                        IORING_SETUP_COOP_TASKRUN;
    constexpr uint32_t queue_depths[] = {IO_QUEUE_DEPTH, 1024, 256};

    int last_error = 0;
    bool ring_ready = false;

    auto try_init_ring = [&](uint32_t depth, unsigned flags) noexcept {
        struct io_uring_params params = {};
        params.flags = flags;

        int ret = io_uring_queue_init_params(depth, &ring_, &params);
        if (ret < 0) {
            last_error = -ret;
            return false;
        }
        return true;
    };

    for (uint32_t depth : queue_depths) {
        if (try_init_ring(depth, advanced_flags) || try_init_ring(depth, 0)) {
            ring_ready = true;
            break;
        }
    }

    if (!ring_ready) {
        throw std::system_error(last_error, std::generic_category(), "io_uring_queue_init_params failed");
    }

    if (int ret = io_uring_register_files_sparse(&ring_, MAX_DIRECT_FDS); ret < 0) {
        throw std::system_error(-ret, std::generic_category(), "io_uring_register_files_sparse failed");
    }
    setup_provided_buffer_ring();
    initialized_ = true;
}

IoEngine::~IoEngine() {
    if (!initialized_) return;
    if (br_) {
        io_uring_free_buf_ring(&ring_, br_, BUF_RING_SIZE, BUF_BGID);
    }
    if (buffer_base_) {
        mi_free_aligned(buffer_base_, BUF_BLOCK_SIZE);
    }
    io_uring_queue_exit(&ring_);
}

void IoEngine::setup_provided_buffer_ring() {
    int err = 0;
    br_ = io_uring_setup_buf_ring(&ring_, BUF_RING_SIZE, BUF_BGID, 0, &err);
    if (!br_) throw std::system_error(-err, std::generic_category(), "io_uring_setup_buf_ring failed");
    io_uring_buf_ring_init(br_);

    buffer_base_ = mi_malloc_aligned(BUF_RING_SIZE * BUF_BLOCK_SIZE, 4096);
    if (!buffer_base_) throw std::bad_alloc();
    std::memset(buffer_base_, 0, BUF_RING_SIZE * BUF_BLOCK_SIZE);

    for (uint16_t i = 0; i < BUF_RING_SIZE; ++i) {
        void* ptr = static_cast<char*>(buffer_base_) + (i * BUF_BLOCK_SIZE);
        io_uring_buf_ring_add(br_, ptr, BUF_BLOCK_SIZE, i, io_uring_buf_ring_mask(BUF_RING_SIZE), i);
    }
    io_uring_buf_ring_advance(br_, BUF_RING_SIZE);
}

void IoEngine::poll() {
    struct io_uring_cqe* cqe;
    unsigned head;
    unsigned advanced = 0;

    auto* scheduler = Scheduler::current();
    if (!scheduler) return;

    io_uring_for_each_cqe(&ring_, head, cqe) {
        metrics_record_io_event();

        uintptr_t ud = reinterpret_cast<uintptr_t>(io_uring_cqe_get_data(cqe));
        uintptr_t tag = ud & TAG_MASK;
        uintptr_t payload = ud & ~TAG_MASK;

        if (tag == TAG_IO && payload) {
            auto* ctx = reinterpret_cast<IoContext*>(reinterpret_cast<void*>(payload));
            if (ctx->cancelled_) {
                delete ctx;
            } else {
                ctx->res = cqe->res;
                ctx->flags = cqe->flags;
                if (ctx->coro_) scheduler->schedule(ctx->coro_);
            }
        }
        else if (tag == TAG_MULTI && payload) {
            auto* op = reinterpret_cast<MultishotOp*>(reinterpret_cast<void*>(payload));
            op->invoke(op, cqe->res, cqe->flags);
        }
        else if (tag == TAG_CORO) {
            auto coro = std::coroutine_handle<>::from_address(reinterpret_cast<void*>(payload));
            scheduler->schedule(coro);
        }
        else if (tag == TAG_INBOX) {
            scheduler->process_inbox();
        }
        ++advanced;
    }

    if (advanced > 0) {
        io_uring_cq_advance(&ring_, advanced);
    }
}

void IoEngine::accept_multishot(int server_fd, MultishotOp* op) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_multishot_accept(sqe, server_fd, nullptr, nullptr, 0);
    sqe->file_index = IORING_FILE_INDEX_ALLOC;
    uintptr_t tagged_ud = reinterpret_cast<uintptr_t>(op) | TAG_MULTI;
    io_uring_sqe_set_data64(sqe, static_cast<uint64_t>(tagged_ud));
}

void IoEngine::recv_multishot(int direct_fd, MultishotOp* op) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_recv_multishot(sqe, direct_fd, nullptr, 0, 0);
    io_uring_sqe_set_flags(sqe, IOSQE_BUFFER_SELECT | IOSQE_FIXED_FILE);
    sqe->buf_group = BUF_BGID;
    uintptr_t tagged_ud = reinterpret_cast<uintptr_t>(op) | TAG_MULTI;
    io_uring_sqe_set_data64(sqe, static_cast<uint64_t>(tagged_ud));
}

void IoEngine::send_msg_ring(int target_ring_fd, uint64_t data) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        io_uring_submit(&ring_);
        sqe = io_uring_get_sqe(&ring_);
    }
    io_uring_prep_msg_ring(sqe, target_ring_fd, 0, data, 0);
    io_uring_sqe_set_data(sqe, nullptr);
}

SafeIoAwaiter IoEngine::accept_direct(int server_fd) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_accept_direct(sqe, server_fd, nullptr, nullptr, 0, IORING_FILE_INDEX_ALLOC);
    auto* ctx = new IoContext{};
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(ctx) | TAG_IO));
    return SafeIoAwaiter{ctx};
}

SafeIoAwaiter IoEngine::recv_direct(int direct_fd) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_recv(sqe, direct_fd, nullptr, BUF_BLOCK_SIZE, 0);
    io_uring_sqe_set_flags(sqe, IOSQE_BUFFER_SELECT | IOSQE_FIXED_FILE);
    sqe->buf_group = BUF_BGID;
    auto* ctx = new IoContext{};
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(ctx) | TAG_IO));
    return SafeIoAwaiter{ctx};
}

SafeIoAwaiter IoEngine::send_direct(int direct_fd, const void* buf, size_t len) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_send(sqe, direct_fd, buf, len, 0);
    io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
    auto* ctx = new IoContext{};
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(ctx) | TAG_IO));
    return SafeIoAwaiter{ctx};
}

SafeIoAwaiter IoEngine::close_direct(int direct_fd) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_close_direct(sqe, direct_fd);
    auto* ctx = new IoContext{};
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(ctx) | TAG_IO));
    return SafeIoAwaiter{ctx};
}

void* IoEngine::get_buffer(uint16_t buffer_id) const noexcept {
    return static_cast<char*>(buffer_base_) + (buffer_id * BUF_BLOCK_SIZE);
}

void IoEngine::return_buffer(uint16_t buffer_id) noexcept {
    void* ptr = get_buffer(buffer_id);
    io_uring_buf_ring_add(br_, ptr, BUF_BLOCK_SIZE, buffer_id, io_uring_buf_ring_mask(BUF_RING_SIZE), br_->tail);
    io_uring_buf_ring_advance(br_, 1);
}

} // namespace novaio
