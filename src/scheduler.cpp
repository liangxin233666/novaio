#include "novaio/scheduler.hpp"
#include "novaio/runtime.hpp"
#include <sched.h>
#include <pthread.h>
#include <numa.h>
#include <chrono>
#include <sys/eventfd.h>
#include <unistd.h>
#include <poll.h>
#include "novaio/metrics.hpp"

namespace novaio {

thread_local mi_heap_t* current_thread_heap = nullptr;
thread_local Scheduler* current_scheduler_ptr = nullptr;
thread_local novaio::TcmMetrics current_thread_metrics;

Scheduler* Scheduler::current() noexcept { return current_scheduler_ptr; }

uint64_t Scheduler::current_time_ms() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

Scheduler::Scheduler(size_t id) : id_(id) {
    wakeup_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
}

Scheduler::~Scheduler() {
    if (wakeup_fd_ >= 0) close(wakeup_fd_);
    if (thread_heap_) mi_heap_delete(thread_heap_);
    MetricsRegistry::get().unregister_thread(&current_thread_metrics);
}

void Scheduler::setup_numa_and_affinity() {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(id_, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    thread_heap_ = mi_heap_new();
    int numa_node = numa_node_of_cpu(id_);
    if (numa_node >= 0) mi_heap_set_numa_affinity(thread_heap_, numa_node);
    current_thread_heap = thread_heap_;
}


void Scheduler::push_inbox(UniqueTask task) {
    if (inbox_.push(std::move(task))) {
        uint64_t val = 1;
        ::write(wakeup_fd_, &val, sizeof(val)); 
    }
}


void Scheduler::process_inbox() {
    UniqueTask task;
    while (inbox_.pop(task)) {
        metrics_record_inbox_processed();
        if (task) task(); 
    }
}

void Scheduler::run() {
    setup_numa_and_affinity();
    io_engine_.init(); 

    current_scheduler_ptr = this;
    MetricsRegistry::get().register_thread(&current_thread_metrics);

    struct io_uring_sqe* sqe = io_uring_get_sqe(io_engine_.raw_ring());
    io_uring_prep_poll_multishot(sqe, wakeup_fd_, POLLIN);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(WAKEUP_USER_DATA));

    std::array<std::coroutine_handle<>, BATCH_PIPELINE_DEPTH> batch;
    uint64_t last_tick_time = current_time_ms();

    while (Runtime::get().is_running_.load(std::memory_order_acquire)) {
        uint64_t now = current_time_ms();
        if (now > last_tick_time) {
            for (uint64_t t = last_tick_time + 1; t <= now; ++t) time_wheel_.tick(t);
            last_tick_time = now;
        }

        process_inbox();

        size_t count = try_dequeue_batch(batch.data(), BATCH_PIPELINE_DEPTH);

        if (count == 0) {
            io_uring_submit(io_engine_.raw_ring());
            process_io_events(); 
            continue; 
        }

        for (size_t i = 0; i < count; ++i) {
            if (i + 1 < count && batch[i + 1]) __builtin_prefetch(batch[i + 1].address(), 1, 3);
            auto handle = batch[i];
            if (handle && !handle.done()) {
                handle.resume(); 
                metrics_record_task_executed();
            }
        }
        io_engine_.poll();
    }
}

void Scheduler::schedule(std::coroutine_handle<> handle) {
    if (!runnext_) {
        runnext_ = handle; 
    } else {
        local_queue_.push(handle); 
    }
}

size_t Scheduler::try_dequeue_batch(std::coroutine_handle<>* batch_out, size_t max_count) {
    size_t count = 0;
    if (runnext_) batch_out[count++] = std::exchange(runnext_, nullptr);
    while (count < max_count) {
        if (auto h = local_queue_.pop()) {
            batch_out[count++] = h;
        } else break;
    }
    return count;
}

void Scheduler::process_io_events() {
    struct io_uring_cqe* cqe;
    uint64_t exact_timeout_ms = time_wheel_.next_timeout(current_time_ms());
    int ret = 0;
    if (exact_timeout_ms == (uint64_t)-1) {
        ret = io_uring_wait_cqe(io_engine_.raw_ring(), &cqe);
    } else if (exact_timeout_ms == 0) {
        ret = io_uring_peek_cqe(io_engine_.raw_ring(), &cqe);
    } else {
        struct __kernel_timespec ts;
        ts.tv_sec = exact_timeout_ms / 1000;
        ts.tv_nsec = (exact_timeout_ms % 1000) * 1000000;
        ret = io_uring_wait_cqe_timeout(io_engine_.raw_ring(), &cqe, &ts);
    }
    if (ret == 0 || ret == -ETIME) {
        io_engine_.poll(); 
        uint64_t val;
        while (::read(wakeup_fd_, &val, sizeof(val)) > 0) {} 
    }
}

} // namespace novaio