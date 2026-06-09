#include "novaio/runtime.hpp"
#include "novaio/io_engine.hpp"
#include <stdexcept>


namespace {
    struct ExtRingGuard {
        struct io_uring ring;
        ExtRingGuard() { io_uring_queue_init(16, &ring, 0); }
        ~ExtRingGuard() { io_uring_queue_exit(&ring); }
    };
    thread_local ExtRingGuard tl_ext_ring;
}

namespace novaio {

void Runtime::start(size_t num_cores) {
    is_running_ = true;
    for (size_t i = 0; i < num_cores; ++i) {
        schedulers_.push_back(std::make_unique<Scheduler>(i));
    }
    for (size_t i = 0; i < num_cores; ++i) {
        workers_.emplace_back([this, i]() {
            schedulers_[i]->run();
        });
    }
}


void Runtime::dispatch_on(size_t core_id, UniqueTask task) {
    if (core_id < schedulers_.size()) {
        auto* target = schedulers_[core_id].get();
        if (target->push_inbox(std::move(task))) {
            int fd = target->ring_fd();
            if (fd >= 0) {
                notify_target_ring(fd, TAG_INBOX);
            }
        }
    }
}

void Runtime::stop() {
    is_running_.store(false, std::memory_order_release);
    for (size_t i = 0; i < schedulers_.size(); ++i) {

        dispatch_on(i, UniqueTask([](){}));
    }
    for (auto& t : workers_) {
        if (t.joinable()) t.join();
    }
}

void Runtime::notify_target_ring(int target_fd, uint64_t msg_data) {
    auto* current_sched = Scheduler::current();
    if (current_sched) {
        current_sched->io_engine().send_msg_ring(target_fd, msg_data);
    } else {
        auto& ring = tl_ext_ring.ring;
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
        if (!sqe) {
            io_uring_submit(&ring);
            sqe = io_uring_get_sqe(&ring);
        }
        if (sqe) {
            io_uring_prep_msg_ring(sqe, target_fd, 0, msg_data, 0);
            io_uring_sqe_set_data(sqe, nullptr);
            io_uring_submit(&ring);
        }
        struct io_uring_cqe* cqe;
        while (io_uring_peek_cqe(&ring, &cqe) == 0) {
            io_uring_cq_advance(&ring, 1);
        }
    }
}

} // namespace novaio