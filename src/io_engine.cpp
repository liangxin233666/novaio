#include "novaio/io_engine.hpp"
#include <cstring>
#include <stdexcept>
#include <sys/socket.h>
#include "novaio/scheduler.hpp"

namespace novaio {

void IoEngine::init() {
    struct io_uring_params params = {};
   
    params.flags |= IORING_SETUP_SINGLE_ISSUER | 
                    IORING_SETUP_DEFER_TASKRUN | 
                    IORING_SETUP_COOP_TASKRUN;
    
    if (int ret = io_uring_queue_init_params(IO_QUEUE_DEPTH, &ring_, &params); ret < 0) {
        throw std::system_error(-ret, std::generic_category(), "io_uring_queue_init_params failed");
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
        uintptr_t ud = reinterpret_cast<uintptr_t>(io_uring_cqe_get_data(cqe));
        
        if (ud & MULTISHOT_FLAG) {
   
            auto cb = reinterpret_cast<MultishotCallback>(ud & ~MULTISHOT_FLAG);
            
       
            cb(cqe->res, cqe->flags);
   
        } 
        else if (ud) {
          
            auto* awaiter = reinterpret_cast<IoAwaiter*>(ud);
            awaiter->res_ = cqe->res;
            awaiter->flags_ = cqe->flags;
            if (awaiter->coro_) {
                scheduler->schedule(awaiter->coro_);
            }
        }
        ++advanced;
    }
    
    if (advanced > 0) {
        io_uring_cq_advance(&ring_, advanced);
    }
}

void IoEngine::accept_multishot(int server_fd, MultishotCallback cb) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_multishot_accept(sqe, server_fd, nullptr, nullptr, 0);
    sqe->file_index = IORING_FILE_INDEX_ALLOC; 

    uintptr_t tagged_ud = reinterpret_cast<uintptr_t>(cb) | MULTISHOT_FLAG;
    io_uring_sqe_set_data64(sqe, static_cast<uint64_t>(tagged_ud));
}


IoAwaiter IoEngine::accept_direct(int server_fd) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_accept_direct(sqe, server_fd, nullptr, nullptr, 0, IORING_FILE_INDEX_ALLOC);
    return IoAwaiter{&ring_, sqe};
}

IoAwaiter IoEngine::recv_direct(int direct_fd) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_recv(sqe, direct_fd, nullptr, BUF_BLOCK_SIZE, 0); 
    io_uring_sqe_set_flags(sqe, IOSQE_BUFFER_SELECT | IOSQE_FIXED_FILE);
    sqe->buf_group = BUF_BGID; 
    return IoAwaiter{&ring_, sqe};
}

IoAwaiter IoEngine::send_direct(int direct_fd, const void* buf, size_t len) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_send(sqe, direct_fd, buf, len, 0);
    io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
    return IoAwaiter{&ring_, sqe};
}

IoAwaiter IoEngine::close_direct(int direct_fd) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    io_uring_prep_close_direct(sqe, direct_fd);
    return IoAwaiter{&ring_, sqe};
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