# NovaIO 项目技术复盘与面试准备

## 1. 项目一句话

NovaIO 是一个基于 C++ 协程、io_uring、每核调度器、Direct FD、Provided Buffer Ring、mimalloc 和层次时间轮实现的用户态异步 I/O 调度框架。

它不是一个普通 echo server，而是一个网络框架底层 runtime 原型。核心目标是把 Linux 高性能 I/O 完成事件和 C++ 协程恢复机制打通，让业务代码用接近同步的写法表达异步网络流程。

面试中可以这样说：

> 我实现了一个 C++ 协程网络 runtime。每个调度线程持有独立的 io_uring、本地任务队列、时间轮和线程本地 allocator。业务协程在发起 I/O 时挂起，io_uring 完成事件到达后通过 user_data 找回上下文，再把 coroutine handle 放回调度器恢复执行。这个项目主要验证了协程调度、io_uring、跨核唤醒、Direct FD、Provided Buffer Ring 和时间轮在高并发网络场景里的组合。

## 2. 整体架构

NovaIO 可以拆成四层：

1. 协程抽象层：`Task<T>`、`Task<void>`、`DetachedTask`。
2. 调度层：`Runtime`、每核 `Scheduler`、本地队列、跨核 inbox、批量 resume。
3. I/O 层：每个 Scheduler 绑定一个 `io_uring`，支持 Direct FD、Provided Buffer Ring、multishot accept/recv、msg_ring。
4. 基础设施层：时间轮、协程锁、Event、when_all、序列化、metrics、mimalloc、NUMA/CPU affinity。

核心请求链路：

1. `Runtime::start(n)` 启动 n 个 Scheduler 线程。
2. 每个 Scheduler 绑定 CPU，创建本地 `mimalloc heap`，初始化独立 `io_uring`。
3. 服务端提交 `accept_direct` 或 `accept_multishot`。
4. 内核完成连接事件后写入 CQE。
5. `IoEngine::poll()` 读取 CQE，根据 `user_data` 低 3 位 tag 判断事件类型。
6. 如果是普通 I/O，找到 `IoContext`，写入 `res/flags`，把协程 handle 放回 Scheduler。
7. Scheduler 下一轮批量 resume 协程，业务继续执行 `recv -> send -> close`。
8. 如果跨核调度，则通过 inbox + `io_uring msg_ring` 唤醒目标 ring。

这条链路是面试中最应该讲熟的主线。

## 3. 核心技术点与关键代码实现

### 3.1 自定义协程 Task

位置：`include/novaio/task.hpp`

你的 `Task<T>` 是自己实现的 C++ coroutine promise，不是直接套库。

关键代码：

```cpp
template <typename T = void>
class [[clang::coro_await_elidable]] Task {
public:
    struct promise_type {
        T value_;
        std::exception_ptr exception_;
        std::coroutine_handle<> continuation_{nullptr};

        Task get_return_object() noexcept {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() noexcept { return {}; }

        struct FinalAwaiter {
            bool await_ready() const noexcept { return false; }

            template <typename Promise>
            std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> h) noexcept {
                if (auto cont = h.promise().continuation_) return cont;
                return std::noop_coroutine();
            }

            void await_resume() noexcept {}
        };

        FinalAwaiter final_suspend() noexcept { return {}; }

        template <typename U>
        void return_value(U&& value) {
            value_ = std::forward<U>(value);
        }

        void unhandled_exception() noexcept {
            exception_ = std::current_exception();
        }
    };
};
```

怎么讲：

> `initial_suspend` 让协程创建后先挂起，由 Scheduler 决定什么时候运行。`final_suspend` 返回 continuation，实现被等待协程结束后自动恢复上层协程。异常通过 `exception_ptr` 保存，在 `await_resume` 里重新抛出。

### 3.2 co_await 如何串起两个协程

位置：`include/novaio/task.hpp`

关键代码：

```cpp
auto operator co_await() noexcept {
    struct Awaiter {
        std::coroutine_handle<promise_type> coro_;

        bool await_ready() const noexcept { return false; }

        std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiting_coro) noexcept {
            coro_.promise().continuation_ = awaiting_coro;
            return coro_;
        }

        T await_resume() {
            if (coro_.promise().exception_) {
                std::rethrow_exception(coro_.promise().exception_);
            }
            return std::move(coro_.promise().value_);
        }
    };

    return Awaiter{coro_};
}
```

怎么讲：

> 当 A `co_await` B 时，A 的 handle 会被保存到 B 的 `continuation_`。然后 `await_suspend` 返回 B 的 handle，调度器继续执行 B。B 完成后在 `final_suspend` 返回 continuation，也就是恢复 A。这个机制把异步调用链串起来了。

### 3.3 协程帧使用 mimalloc 分配

位置：`include/novaio/task.hpp`

关键代码：

```cpp
extern thread_local mi_heap_t* current_thread_heap;

void* operator new(std::size_t size) noexcept {
    return current_thread_heap
        ? mi_heap_malloc(current_thread_heap, size)
        : mi_malloc(size);
}

void operator delete(void* ptr, std::size_t size) noexcept {
    mi_free_size(ptr, size);
}
```

Scheduler 初始化线程本地 heap：

```cpp
void Scheduler::setup_numa_and_affinity() {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(id_, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    thread_heap_ = mi_heap_new();

    int numa_node = numa_node_of_cpu(id_);
    if (numa_node >= 0) {
        mi_heap_set_numa_affinity(thread_heap_, numa_node);
    }

    current_thread_heap = thread_heap_;
}
```

怎么讲：

> 协程帧创建和销毁很频繁。如果走全局 allocator，高并发下容易产生锁竞争和内存抖动。所以我给每个 Scheduler 线程绑定一个 mimalloc heap，协程帧优先从线程本地 heap 分配。

### 3.4 每核一个 Scheduler

位置：`src/runtime.cpp`

关键代码：

```cpp
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
```

怎么讲：

> Runtime 按 core 数创建 Scheduler，每个 Scheduler 在线程里独立运行。这个设计接近 shard-per-core，每个核心维护自己的任务队列、io_uring、时间轮和 allocator，减少共享状态。

### 3.5 Scheduler 主循环

位置：`src/scheduler.cpp`

关键代码：

```cpp
void Scheduler::run() {
    setup_numa_and_affinity();
    io_engine_.init();

    current_scheduler_ptr = this;
    MetricsRegistry::get().register_thread(&current_thread_metrics);

    std::array<std::coroutine_handle<>, BATCH_PIPELINE_DEPTH> batch;
    uint64_t last_tick_time = current_time_ms();

    while (Runtime::get().is_running_.load(std::memory_order_acquire)) {
        uint64_t now = current_time_ms();

        if (now > last_tick_time) {
            for (uint64_t t = last_tick_time + 1; t <= now; ++t) {
                time_wheel_.tick(t);
            }
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
            if (i + 1 < count && batch[i + 1]) {
                __builtin_prefetch(batch[i + 1].address(), 1, 3);
            }

            auto handle = batch[i];
            if (handle && !handle.done()) {
                handle.resume();
                metrics_record_task_executed();
            }
        }

        io_engine_.poll();
    }
}
```

怎么讲：

> 主循环做四件事：推进时间轮、处理跨核 inbox、批量恢复协程、处理 io_uring 完成事件。没有任务时会提交 SQE 并进入 io_uring wait，而不是一直空转。

### 3.6 runnext fast path 和本地队列

位置：`src/scheduler.cpp`

关键代码：

```cpp
void Scheduler::schedule(std::coroutine_handle<> handle) {
    if (!runnext_) {
        runnext_ = handle;
    } else {
        local_queue_.push(handle);
    }
}

size_t Scheduler::try_dequeue_batch(
    std::coroutine_handle<>* batch_out,
    size_t max_count
) {
    size_t count = 0;

    if (runnext_) {
        batch_out[count++] = std::exchange(runnext_, nullptr);
    }

    while (count < max_count) {
        if (auto h = local_queue_.pop()) {
            batch_out[count++] = h;
        } else {
            break;
        }
    }

    return count;
}
```

怎么讲：

> 我没有所有任务都直接进队列，而是先放一个 `runnext_`，相当于下一个立即执行槽位。这样常见场景可以减少一次 ring queue 操作。后续任务再进本地队列，批量取出来 resume。

### 3.7 SPSC 跨线程 inbox

位置：`include/novaio/concurrent_queues.hpp`

关键代码：

```cpp
template <typename T>
class SpscQueue {
public:
    bool push(T item) {
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        std::size_t next_tail = tail + 1;

        if (next_tail - head_.load(std::memory_order_acquire) > capacity_) {
            return false;
        }

        new (&buffer_[tail & mask_]) T(std::move(item));
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    bool pop(T& item_out) {
        std::size_t head = head_.load(std::memory_order_relaxed);

        if (head == tail_.load(std::memory_order_acquire)) {
            return false;
        }

        item_out = std::move(buffer_[head & mask_]);
        buffer_[head & mask_].~T();
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

private:
    alignas(CACHE_LINE) std::atomic<std::size_t> tail_{0};
    alignas(CACHE_LINE) std::atomic<std::size_t> head_{0};
};
```

怎么讲：

> inbox 用 SPSC ring buffer，push/pop 用 acquire-release 保证可见性，head 和 tail 分别 cache line 对齐，减少 false sharing。这个实现性能高，但生产化要注意：如果多个线程同时向同一个 Scheduler 投递，它就不再满足 SPSC 假设，需要升级成 MPSC。

### 3.8 跨核投递 + msg_ring 唤醒

位置：`src/runtime.cpp`

关键代码：

```cpp
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
```

唤醒目标 ring：

```cpp
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
    }
}
```

怎么讲：

> 跨核投递分两步：队列传递任务，msg_ring 唤醒目标 io_uring。如果只有队列，目标线程可能阻塞在 `io_uring_wait_cqe`；如果只有 msg_ring，没有队列，就无法传复杂任务对象。所以二者配合。

### 3.9 io_uring 初始化

位置：`src/io_engine.cpp`

关键代码：

```cpp
void IoEngine::init() {
    struct io_uring_params params = {};

    params.flags |= IORING_SETUP_SINGLE_ISSUER |
                    IORING_SETUP_DEFER_TASKRUN |
                    IORING_SETUP_COOP_TASKRUN;

    if (int ret = io_uring_queue_init_params(IO_QUEUE_DEPTH, &ring_, &params); ret < 0) {
        throw std::system_error(-ret, std::generic_category(),
                                "io_uring_queue_init_params failed");
    }

    if (int ret = io_uring_register_files_sparse(&ring_, MAX_DIRECT_FDS); ret < 0) {
        throw std::system_error(-ret, std::generic_category(),
                                "io_uring_register_files_sparse failed");
    }

    setup_provided_buffer_ring();
    initialized_ = true;
}
```

怎么讲：

> 每个 Scheduler 初始化自己的 io_uring。`SINGLE_ISSUER` 表示主要由单线程提交，减少同步开销。`DEFER_TASKRUN` 和 `COOP_TASKRUN` 是为了配合 io_uring 的 taskrun 机制，降低内核和用户态切换成本。然后注册 sparse fixed file table，为 Direct FD 做准备。

### 3.10 Provided Buffer Ring

位置：`src/io_engine.cpp`

关键代码：

```cpp
void IoEngine::setup_provided_buffer_ring() {
    int err = 0;

    br_ = io_uring_setup_buf_ring(&ring_, BUF_RING_SIZE, BUF_BGID, 0, &err);
    if (!br_) {
        throw std::system_error(-err, std::generic_category(),
                                "io_uring_setup_buf_ring failed");
    }

    io_uring_buf_ring_init(br_);

    buffer_base_ = mi_malloc_aligned(BUF_RING_SIZE * BUF_BLOCK_SIZE, 4096);
    if (!buffer_base_) throw std::bad_alloc();

    std::memset(buffer_base_, 0, BUF_RING_SIZE * BUF_BLOCK_SIZE);

    for (uint16_t i = 0; i < BUF_RING_SIZE; ++i) {
        void* ptr = static_cast<char*>(buffer_base_) + (i * BUF_BLOCK_SIZE);

        io_uring_buf_ring_add(
            br_,
            ptr,
            BUF_BLOCK_SIZE,
            i,
            io_uring_buf_ring_mask(BUF_RING_SIZE),
            i
        );
    }

    io_uring_buf_ring_advance(br_, BUF_RING_SIZE);
}
```

归还 buffer：

```cpp
void IoEngine::return_buffer(uint16_t buffer_id) noexcept {
    void* ptr = get_buffer(buffer_id);

    io_uring_buf_ring_add(
        br_,
        ptr,
        BUF_BLOCK_SIZE,
        buffer_id,
        io_uring_buf_ring_mask(BUF_RING_SIZE),
        br_->tail
    );

    io_uring_buf_ring_advance(br_, 1);
}
```

怎么讲：

> recv 时我不临时 malloc buffer，而是预注册一组固定 buffer。内核完成 recv 后通过 CQE flags 告诉我用了哪个 buffer id。业务处理完后必须归还。这个设计减少内存分配和数据结构管理开销，但对生命周期要求很高。

### 3.11 Direct FD accept / recv / send

位置：`src/io_engine.cpp`

accept：

```cpp
SafeIoAwaiter IoEngine::accept_direct(int server_fd) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);

    io_uring_prep_accept_direct(
        sqe,
        server_fd,
        nullptr,
        nullptr,
        0,
        IORING_FILE_INDEX_ALLOC
    );

    auto* ctx = new IoContext{};

    io_uring_sqe_set_data(
        sqe,
        reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(ctx) | TAG_IO)
    );

    return SafeIoAwaiter{ctx};
}
```

recv：

```cpp
SafeIoAwaiter IoEngine::recv_direct(int direct_fd) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);

    io_uring_prep_recv(sqe, direct_fd, nullptr, BUF_BLOCK_SIZE, 0);
    io_uring_sqe_set_flags(sqe, IOSQE_BUFFER_SELECT | IOSQE_FIXED_FILE);
    sqe->buf_group = BUF_BGID;

    auto* ctx = new IoContext{};

    io_uring_sqe_set_data(
        sqe,
        reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(ctx) | TAG_IO)
    );

    return SafeIoAwaiter{ctx};
}
```

send：

```cpp
SafeIoAwaiter IoEngine::send_direct(int direct_fd, const void* buf, size_t len) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);

    io_uring_prep_send(sqe, direct_fd, buf, len, 0);
    io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);

    auto* ctx = new IoContext{};

    io_uring_sqe_set_data(
        sqe,
        reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(ctx) | TAG_IO)
    );

    return SafeIoAwaiter{ctx};
}
```

怎么讲：

> accept 得到的是 direct fd，不是传统 fd。后续 recv/send 都加 `IOSQE_FIXED_FILE`，表示从 fixed file table 取 fd。这样能减少普通 fd table 查找成本。

### 3.12 CQE user_data 打 tag 分发

位置：`src/io_engine.cpp`

关键代码：

```cpp
void IoEngine::poll() {
    struct io_uring_cqe* cqe;
    unsigned head;
    unsigned advanced = 0;

    auto* scheduler = Scheduler::current();
    if (!scheduler) return;

    io_uring_for_each_cqe(&ring_, head, cqe) {
        uintptr_t ud = reinterpret_cast<uintptr_t>(io_uring_cqe_get_data(cqe));
        uintptr_t tag = ud & TAG_MASK;
        uintptr_t payload = ud & ~TAG_MASK;

        if (tag == TAG_IO && payload) {
            auto* ctx = reinterpret_cast<IoContext*>(
                reinterpret_cast<void*>(payload)
            );

            if (ctx->cancelled_) {
                delete ctx;
            } else {
                ctx->res = cqe->res;
                ctx->flags = cqe->flags;

                if (ctx->coro_) {
                    scheduler->schedule(ctx->coro_);
                }
            }
        } else if (tag == TAG_MULTI && payload) {
            auto* op = reinterpret_cast<MultishotOp*>(
                reinterpret_cast<void*>(payload)
            );

            op->invoke(op, cqe->res, cqe->flags);
        } else if (tag == TAG_CORO) {
            auto coro = std::coroutine_handle<>::from_address(
                reinterpret_cast<void*>(payload)
            );

            scheduler->schedule(coro);
        } else if (tag == TAG_INBOX) {
            scheduler->process_inbox();
        }

        ++advanced;
    }

    if (advanced > 0) {
        io_uring_cq_advance(&ring_, advanced);
    }
}
```

怎么讲：

> 我把 `user_data` 低 3 位作为 tag，高位作为 payload。这样同一个 CQE 处理函数可以分发普通 I/O、multishot 回调、跨核协程唤醒和 inbox 消息。这个做法要求 payload 指针按 8 字节对齐。

### 3.13 SafeIoAwaiter：I/O 完成后恢复协程

位置：`include/novaio/io_context.hpp`

关键代码：

```cpp
struct SafeIoAwaiter {
    IoContext* ctx_;

    explicit SafeIoAwaiter(IoContext* ctx) : ctx_(ctx) {}

    ~SafeIoAwaiter() {
        if (ctx_) ctx_->cancelled_ = true;
    }

    bool await_ready() const noexcept {
        return false;
    }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        ctx_->coro_ = h;
    }

    IoResult await_resume() {
        IoResult ret = {ctx_->res, ctx_->flags};
        delete ctx_;
        ctx_ = nullptr;
        return ret;
    }
};
```

怎么讲：

> 发起 I/O 时创建 `IoContext`，协程挂起时把自己的 handle 存进去。CQE 到来后 `poll()` 填充结果并调度这个 handle。协程恢复后 `await_resume()` 返回 I/O 结果。

### 3.14 multishot accept / recv

位置：`src/io_engine.cpp`

关键代码：

```cpp
void IoEngine::accept_multishot(int server_fd, MultishotOp* op) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);

    io_uring_prep_multishot_accept(sqe, server_fd, nullptr, nullptr, 0);
    sqe->file_index = IORING_FILE_INDEX_ALLOC;

    uintptr_t tagged_ud = reinterpret_cast<uintptr_t>(op) | TAG_MULTI;
    io_uring_sqe_set_data64(sqe, static_cast<uint64_t>(tagged_ud));
}
```

```cpp
void IoEngine::recv_multishot(int direct_fd, MultishotOp* op) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);

    io_uring_prep_recv_multishot(sqe, direct_fd, nullptr, 0, 0);
    io_uring_sqe_set_flags(sqe, IOSQE_BUFFER_SELECT | IOSQE_FIXED_FILE);
    sqe->buf_group = BUF_BGID;

    uintptr_t tagged_ud = reinterpret_cast<uintptr_t>(op) | TAG_MULTI;
    io_uring_sqe_set_data64(sqe, static_cast<uint64_t>(tagged_ud));
}
```

怎么讲：

> multishot 的好处是一次提交可以产生多个完成事件，减少重复提交 SQE 的开销。我的代码用 `MultishotOp` 保存回调入口，CQE 到来后调用 `op->invoke(op, res, flags)`。

### 3.15 层次时间轮

位置：`src/time_wheel.cpp`

关键代码：

```cpp
void TimeWheel::add_timer(TimerNode* node) {
    active_timers_++;

    uint64_t expires = node->expires_at;
    uint64_t idx = expires - current_tick_;
    TimerNode** head = nullptr;

    if (idx < TVR_SIZE) {
        head = &tv1_[expires & TVR_MASK];
    } else if (idx < (1ULL << (TVR_BITS + TVN_BITS))) {
        head = &tv2_[(expires >> TVR_BITS) & TVN_MASK];
    } else if (idx < (1ULL << (TVR_BITS + 2 * TVN_BITS))) {
        head = &tv3_[(expires >> (TVR_BITS + TVN_BITS)) & TVN_MASK];
    } else if (idx < (1ULL << (TVR_BITS + 3 * TVN_BITS))) {
        head = &tv4_[(expires >> (TVR_BITS + 2 * TVN_BITS)) & TVN_MASK];
    } else {
        head = &tv5_[(expires >> (TVR_BITS + 3 * TVN_BITS)) & TVN_MASK];
    }

    node->next = *head;
    if (*head) (*head)->prev = node;

    node->prev = nullptr;
    node->head_ptr = head;
    *head = node;
}
```

tick 触发：

```cpp
void TimeWheel::tick(uint64_t current_time) {
    while (current_tick_ < current_time) {
        uint32_t index = current_tick_ & TVR_MASK;

        if (!index && current_tick_ > 0) {
            uint32_t i2 = (current_tick_ >> TVR_BITS) & TVN_MASK;
            cascade(tv2_, i2);

            if (!i2) {
                uint32_t i3 = (current_tick_ >> (TVR_BITS + TVN_BITS)) & TVN_MASK;
                cascade(tv3_, i3);
            }
        }

        TimerNode* curr = tv1_[index];
        tv1_[index] = nullptr;

        while (curr) {
            TimerNode* next = curr->next;
            curr->head_ptr = nullptr;

            if (Scheduler::current()) {
                Scheduler::current()->schedule(curr->handle);
            }

            curr = next;
        }

        current_tick_++;
    }
}
```

`sleep_for`：

```cpp
void SleepAwaiter::await_suspend(std::coroutine_handle<> h) noexcept {
    auto scheduler = Scheduler::current();

    if (scheduler) {
        node_.expires_at = scheduler->current_time_ms() + ms_;
        node_.handle = h;
        scheduler->time_wheel().add_timer(&node_);
    }
}
```

怎么讲：

> `sleep_for` 不阻塞线程，而是把当前协程挂到时间轮。Scheduler 每轮推进 tick，到期后把协程重新 schedule。时间轮适合大量 timeout，插入接近 O(1)。

### 3.16 CoMutex 协程锁

位置：`src/comutex.cpp`

加锁关键代码：

```cpp
bool CoMutex::Awaiter::await_suspend(std::coroutine_handle<> h) noexcept {
    awaiting_coroutine_ = h;
    scheduler_ = Scheduler::current();

    uintptr_t old_state = mutex_.state_.load(std::memory_order_acquire);

    while (true) {
        if (old_state == UNLOCKED) {
            if (mutex_.state_.compare_exchange_weak(
                    old_state,
                    LOCKED_NO_WAITERS,
                    std::memory_order_acquire,
                    std::memory_order_relaxed)) {
                return false;
            }
        } else {
            next_ = reinterpret_cast<Awaiter*>(
                old_state == LOCKED_NO_WAITERS ? 0 : old_state
            );

            if (mutex_.state_.compare_exchange_weak(
                    old_state,
                    reinterpret_cast<uintptr_t>(this),
                    std::memory_order_release,
                    std::memory_order_relaxed)) {
                return true;
            }
        }
    }
}
```

解锁关键代码：

```cpp
void CoMutex::unlock() noexcept {
    if (waiters_head_ == nullptr) {
        uintptr_t old_state = state_.exchange(
            LOCKED_NO_WAITERS,
            std::memory_order_acquire
        );

        if (old_state == LOCKED_NO_WAITERS) {
            if (state_.compare_exchange_strong(
                    old_state,
                    UNLOCKED,
                    std::memory_order_release,
                    std::memory_order_relaxed)) {
                return;
            }

            old_state = state_.exchange(
                LOCKED_NO_WAITERS,
                std::memory_order_acquire
            );
        }

        Awaiter* current = reinterpret_cast<Awaiter*>(old_state);
        Awaiter* prev = nullptr;

        while (current != nullptr) {
            Awaiter* next = current->next_;
            current->next_ = prev;
            prev = current;
            current = next;
        }

        waiters_head_ = prev;
    }

    Awaiter* to_resume = waiters_head_;
    waiters_head_ = to_resume->next_;

    auto* current_sched = Scheduler::current();
    auto* target_sched = to_resume->scheduler_;

    if (target_sched == current_sched && current_sched != nullptr) {
        current_sched->schedule(to_resume->awaiting_coroutine_);
    } else if (target_sched != nullptr) {
        uintptr_t coro_addr =
            reinterpret_cast<uintptr_t>(to_resume->awaiting_coroutine_.address());

        Runtime::get().notify_target_ring(
            target_sched->ring_fd(),
            coro_addr | TAG_CORO
        );
    }
}
```

怎么讲：

> 这个锁不会阻塞线程。抢不到锁时协程挂起，awaiter 节点进入等待链表。unlock 时恢复一个等待协程；如果等待协程属于别的 Scheduler，就通过 msg_ring 跨核唤醒。

### 3.17 Event

位置：`include/novaio/event.hpp`

关键代码：

```cpp
void set() noexcept {
    uintptr_t old = state_.exchange(SET, std::memory_order_acq_rel);

    if (old != NOT_SET && old != SET) {
        auto coro = std::coroutine_handle<>::from_address(
            reinterpret_cast<void*>(old)
        );

        auto* current = Scheduler::current();
        if (current) {
            current->schedule(coro);
        }
    }
}
```

等待：

```cpp
bool await_suspend(std::coroutine_handle<> h) noexcept {
    uintptr_t expected = NOT_SET;

    return event_.state_.compare_exchange_strong(
        expected,
        reinterpret_cast<uintptr_t>(h.address()),
        std::memory_order_release,
        std::memory_order_acquire
    );
}
```

怎么讲：

> Event 是 one-shot 信号量。状态要么是未触发，要么是已触发，要么存一个等待协程地址。set 时把等待协程取出来 schedule。

### 3.18 when_all

位置：`include/novaio/when_all.hpp`

关键代码：

```cpp
struct WhenAllCounter {
    std::atomic<size_t> count_;
    std::coroutine_handle<> continuation_{nullptr};

    explicit WhenAllCounter(size_t c) noexcept : count_(c) {}

    void notify() noexcept {
        if (count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            if (continuation_) {
                auto* sched = Scheduler::current();

                if (sched) {
                    sched->schedule(continuation_);
                } else {
                    continuation_.resume();
                }
            }
        }
    }
};
```

启动多个 task：

```cpp
template <std::size_t... Is>
void start_tasks(std::index_sequence<Is...>) {
    (
        detail::run_task(
            std::get<Is>(tasks_),
            std::get<Is>(results_),
            counter_
        ),
        ...
    );
}
```

怎么讲：

> `when_all` 保存多个 task 和结果，给每个 task 包一层 helper 协程。每完成一个 task 就 `counter--`，最后一个完成时恢复等待 `when_all` 的 continuation。

### 3.19 HTTP echo 示例里的完整 I/O 链路

位置：`examples/http_server.cpp`

关键代码：

```cpp
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
            direct_fd,
            HTTP_RESPONSE.data(),
            HTTP_RESPONSE.size()
        );

        if (send_result.res < 0) break;
    }

    co_await scheduler->io_engine().close_direct(direct_fd);
}
```

怎么讲：

> 业务代码看起来像同步代码：`recv -> send -> close`，但每一步都是异步 io_uring 操作。协程在 I/O 未完成时挂起，线程继续执行其他协程，完成后再恢复。

### 3.20 序列化器

位置：`include/novaio/serializer.hpp`

关键代码：

```cpp
template <typename T>
void serialize(const T& obj, std::vector<char>& buffer) {
    if constexpr (std::is_arithmetic_v<T>) {
        append_bytes(buffer, obj);
    } else if constexpr (std::is_same_v<T, std::string>) {
        size_t len = obj.size();
        append_bytes(buffer, len);
        buffer.insert(buffer.end(), obj.begin(), obj.end());
    } else {
        auto tuple_view = obj.tie();
        serialize_tuple_impl(
            tuple_view,
            buffer,
            std::make_index_sequence<std::tuple_size_v<decltype(tuple_view)>>{}
        );
    }
}
```

怎么讲：

> 这个序列化器用模板分发处理基本类型、字符串和提供 `tie()` 的结构体。它是一个轻量示例，说明框架可以支持自定义协议编码。但生产化还要考虑大小端、版本兼容、反序列化、边界检查和跨语言协议。

### 3.21 Metrics

位置：`include/novaio/metrics.hpp`

关键代码：

```cpp
struct alignas(CACHE_LINE) TcmMetrics {
    uint64_t tasks_executed{0};
    uint64_t io_events_reaped{0};
    uint64_t inbox_messages_processed{0};
};

extern thread_local TcmMetrics current_thread_metrics;

inline void metrics_record_task_executed() noexcept {
#ifdef NOVAIO_ENABLE_METRICS
    current_thread_metrics.tasks_executed++;
#endif
}
```

汇总：

```cpp
inline TcmMetrics aggregate() const {
    TcmMetrics total{};
#ifdef NOVAIO_ENABLE_METRICS
    std::shared_lock lock(rw_mutex_);
    for (const auto* m : thread_metrics_) {
        total.tasks_executed += m->tasks_executed;
        total.io_events_reaped += m->io_events_reaped;
        total.inbox_messages_processed += m->inbox_messages_processed;
    }
#endif
    return total;
}
```

怎么讲：

> 每个线程维护自己的 metrics，避免所有线程同时写全局计数器。汇总时再用 registry 聚合。当前版本还有不足：`io_events_reaped` 的记录函数没有真正接入 `IoEngine::poll()`，所以综合测试里 I/O 计数为 0，这个是后续要修的点。

## 4. 项目的优点

1. 技术链路完整：不是只写了业务 demo，而是从协程 promise、调度器、io_uring、跨核唤醒、时间轮到同步原语都有实现。
2. 性能意识强：用了 per-core Scheduler、线程本地 heap、Direct FD、Provided Buffer Ring、multishot、批量调度、cache line 对齐。
3. 异步表达清晰：业务层可以写成 `co_await recv -> co_await send`，比回调嵌套更可读。
4. 贴近现代 Linux 网络 I/O：直接探索 io_uring 新能力，而不是只停留在 epoll。
5. 可解释性强：代码规模不大，但关键路径完整，适合面试中深入讲实现。
6. 有测试验证：覆盖 Task 返回值和异常、when_all、TimeWheel、CoMutex、Event、SPSC、序列化、Direct FD、Provided Buffer Ring、TCP echo。

## 5. 项目的不足和风险

这些点不要回避。主动讲出来，面试官会觉得你清楚工程边界。

1. `SpscQueue` 只适合单生产者单消费者，但 `dispatch_on` 可能被多个外部线程同时调同一个 core，生产化应改成 MPSC。
2. `LocalQueue` 没有满队列检查，压力下可能覆盖旧任务。
3. `dispatch_on` 在目标 ring 还没初始化时可能只入队但无法 msg_ring 唤醒，存在启动阶段竞态。
4. `SafeIoAwaiter` 的 `cancelled_` 只是避免恢复协程，没有真正取消内核 I/O。
5. I/O cancel、连接关闭、direct fd 生命周期、buffer 归还还不够严谨。
6. `TimeWheel::active_timers_` 在 cascade 和触发时统计不严谨，可能导致 `next_timeout` 判断不准确。
7. `Event` 只支持单 waiter，而且跨 Scheduler 场景语义不完整。
8. `when_all` 里 helper 协程异常直接 terminate，生产化需要异常聚合或取消其他任务。
9. 没有 work stealing，某个 core 负载过高时不能自动均衡。
10. 没有完整背压机制，例如 SQE 不足、buffer ring 耗尽、发送缓冲区积压时缺少统一策略。
11. metrics 还比较原型化，`io_events_reaped` 当前没有接入 poll 路径。
12. CMake 目前主要定义 interface library，没有把 src/examples/tests 组织成完整 target，工程化不足。

## 6. 和成熟框架的对比

### 6.1 和 Boost.Asio 对比

Boost.Asio 的优势：

1. 跨平台成熟，支持 epoll/kqueue/IOCP。
2. executor、strand、completion token、cancellation、work guard 等抽象完整。
3. 错误码、生命周期、异步组合操作非常成熟。

NovaIO 的优势：

1. 更贴近 Linux io_uring，直接使用 Direct FD、Provided Buffer Ring、msg_ring。
2. 每核调度和内存分配策略更直接，抽象层更薄。
3. C++ 协程路径更可控，能解释到底层恢复机制。

可以学习 Asio：

1. executor 模型。
2. cancellation slot。
3. strand 串行化语义。
4. 错误处理和资源生命周期。

面试说法：

> Asio 是成熟工程框架，NovaIO 是面向 Linux io_uring 的性能探索原型。我的实现更直接、更底层，但工程完整性和稳定性明显不如 Asio。

### 6.2 和 libuv 对比

libuv 的优势：

1. 跨平台事件循环成熟。
2. handle 生命周期清晰。
3. 文件、网络、timer、signal 等事件统一。

NovaIO 的优势：

1. C++ 协程表达更自然，不需要回调层层传递。
2. 直接利用 io_uring，能把 accept/recv/send/close 都统一成完成事件。

可以学习 libuv：

1. loop stop/restart 语义。
2. handle close 流程。
3. 统一资源对象模型。

### 6.3 和 Tokio 对比

Tokio 的优势：

1. 调度器成熟，有任务预算和 cooperative scheduling。
2. tracing、metrics、backpressure、cancellation 都比较完整。
3. 生态丰富。

NovaIO 的优势：

1. C++ 零成本抽象潜力。
2. 对 io_uring 细节控制更直接。
3. 可以手动控制内存分配、CPU affinity、NUMA。

可以学习 Tokio：

1. cooperative budget，防止单个任务长时间占用线程。
2. tracing 和可观测性。
3. cancellation safety。
4. 背压体系。

### 6.4 和 Seastar 对比

Seastar 是最接近 NovaIO 思路的成熟框架。

共同点：

1. shard-per-core / shared-nothing 思路。
2. 强调每核本地性。
3. 跨 shard 通信。
4. 面向高性能网络服务。

Seastar 的优势：

1. 生产级 reactor。
2. 内存资源隔离和调度组。
3. future 生命周期成熟。
4. 背压和任务分类更完整。

NovaIO 的优势：

1. 代码更小，关键路径可解释。
2. 更适合作为学习和展示 C++ 协程 + io_uring 的项目。
3. 直接使用现代 io_uring 特性。

可以学习 Seastar：

1. 跨 shard 调用模型。
2. reactor 级别的资源管理。
3. 内存池和 allocator 体系。
4. 调度组和服务质量控制。

### 6.5 和 nginx 对比

nginx 的优势：

1. 生产稳定性极强。
2. master-worker 进程模型成熟。
3. reload、日志、连接管理、accept 策略成熟。

NovaIO 的优势：

1. 现代 C++ 协程表达异步流程。
2. io_uring 完成事件模型更强。
3. 用户态协程调度更适合复杂异步业务编排。

可以学习 nginx：

1. 平滑重启。
2. worker 进程管理。
3. 连接限流。
4. 配置热加载。
5. 线上稳定性工程。

### 6.6 和 Go runtime 对比

Go 的优势：

1. goroutine 是语言级能力。
2. netpoll、抢占、栈增长、GC 都由 runtime 管理。
3. 开发效率高。

NovaIO 的优势：

1. 没有 GC 停顿。
2. C++ 可以更细地控制内存、I/O、CPU affinity。
3. 更适合展示底层系统能力。

NovaIO 的短板：

1. 没有抢占。
2. 没有完整 runtime 安全兜底。
3. 生命周期和错误处理都要自己做。

## 7. 后续优化路线

### 7.1 第一阶段：修正确认性问题

1. 把 inbox 从 SPSC 改成 MPSC。
2. 给 `LocalQueue` 加满队列检查。
3. 处理 `io_uring_get_sqe()` 返回空的情况。
4. 修正 `TimeWheel::active_timers_` 统计。
5. 把 `metrics_record_io_event()` 接入 `IoEngine::poll()`。
6. 启动阶段增加 Scheduler ready 同步，避免 ring 尚未初始化时丢唤醒。

### 7.2 第二阶段：资源生命周期生产化

1. 封装 `Connection` 对象。
2. direct fd 使用 RAII 管理。
3. buffer id 使用 RAII 管理，析构自动归还。
4. I/O awaiter 支持真正 cancel。
5. close 路径统一处理半关闭、错误、超时。

### 7.3 第三阶段：调度能力增强

1. 加 cooperative budget，防止单个协程长时间运行。
2. 对 CPU 密集任务投递到专门线程池。
3. 引入 work stealing 或负载均衡。
4. 按任务类型做优先级。
5. 对 I/O 任务保持连接亲和性。

### 7.4 第四阶段：后台服务能力

1. 限流。
2. 熔断。
3. 背压。
4. 连接数上限。
5. 心跳超时。
6. 优雅退出。
7. 日志 trace。
8. Prometheus metrics。

### 7.5 第五阶段：工程体系

1. 正规化 CMake target。
2. 加 CI。
3. 加 ASAN/TSAN。
4. 加单测和集成测试。
5. 加 benchmark 复现脚本。
6. 加 perf/flamegraph 分析。
7. 完善 README 架构图和设计文档。

## 8. 和腾讯企业微信后台开发的关联

企业微信后台常见场景：

1. 长连接网关。
2. 消息推送。
3. 群消息 fanout。
4. 在线状态。
5. 心跳超时。
6. 回调服务。
7. RPC 服务。
8. 限流与可靠投递。

NovaIO 对应到底层接入层能力：

1. 长连接网关需要高并发连接管理，NovaIO 的 io_uring + 协程适合这个方向。
2. 消息推送需要低延迟唤醒，msg_ring 跨核通知可以类比跨 worker 投递。
3. 心跳和超时管理需要大量 timer，时间轮可以支撑。
4. 连接上的请求处理需要异步 I/O，协程能降低业务代码复杂度。
5. 高并发场景需要减少内存抖动，per-thread heap 和 buffer ring 是优化方向。
6. 线上系统需要稳定性，所以要主动说明当前项目是 runtime 原型，生产化要补生命周期、背压和可观测性。

推荐面试表达：

> 企业微信后台尤其关注高并发连接、低延迟消息收发、稳定性和资源利用率。我的项目虽然不是 IM 业务本身，但底层能力对应长连接接入层：事件驱动 I/O、连接管理、超时控制、跨线程调度、内存池化和性能压测。我能从这个项目延伸到后台服务的限流、背压、心跳、可靠投递和可观测性。

## 9. 面试主线

建议按这个顺序讲：

1. 背景：我想做一个 C++ 协程异步网络 runtime。
2. 架构：Runtime 多核启动，每核 Scheduler，每个 Scheduler 持有 io_uring、队列、时间轮、allocator。
3. 核心链路：业务 `co_await recv_direct()` 挂起，I/O 完成后 CQE 找回 `IoContext`，恢复协程。
4. 性能优化：Direct FD、Provided Buffer Ring、multishot、msg_ring、mimalloc、CPU affinity、批量调度。
5. 测试验证：综合测试覆盖基础协程、同步原语、时间轮、网络 echo。
6. 反思不足：MPSC、取消语义、背压、生命周期、可观测性。
7. 和业务关联：长连接网关、消息推送、心跳超时、接入层。

## 10. 面试官可能问的问题与回答

### 10.1 你这个项目解决什么问题？

答：

> 我做的是一个 C++ 协程网络 runtime，核心问题是把异步 I/O 完成事件低成本映射回协程执行。每个调度线程维护自己的 io_uring、局部队列、时间轮和 allocator heap。I/O 完成后通过 user_data tag 找到上下文，再把 coroutine handle 放回调度器恢复。

### 10.2 为什么用 io_uring，不用 epoll？

答：

> epoll 只告诉我 fd 可读写，真正 read/write 还要系统调用；io_uring 可以把 accept、recv、send、close 都提交给内核，完成后统一从 CQE 获取结果。我的实现还用了 Direct FD 和 Provided Buffer Ring，进一步减少 fd 查找和用户态 buffer 管理开销。

### 10.3 协程是怎么恢复的？

答：

> `Task` 的 awaiter 在 `await_suspend` 里记录 continuation，然后返回被等待任务的 coroutine handle；I/O awaiter 则把当前协程 handle 存进 `IoContext`，CQE 到达时 `poll()` 找回 `IoContext`，写入结果并 `scheduler->schedule(ctx->coro_)`，下一轮调度恢复。

### 10.4 为什么每个 Scheduler 一个 io_uring，而不是全局一个？

答：

> 全局一个 ring 会有多线程提交竞争，CQE 分发也要跨线程同步。每核一个 ring 可以让提交、完成、调度都尽量在同一个线程内完成，符合 shared-nothing 思路。代价是跨核连接迁移和负载均衡更复杂。

### 10.5 跨线程怎么投递任务？

答：

> 业务通过 `Runtime::dispatch_on(core, UniqueTask)` 推到目标 Scheduler 的 inbox，然后用 `io_uring msg_ring` 给目标 ring 发消息。目标 ring 收到 `TAG_INBOX` 后处理 inbox，这样目标线程即使阻塞在 io_uring wait 里也能被唤醒。

### 10.6 `SpscQueue` 为什么不够？

答：

> SPSC 假设只有一个生产者和一个消费者。但 `dispatch_on` 可能从多个业务线程或多个 Scheduler 同时投递到同一个目标 core，这时 tail 会被多个生产者竞争，SPSC 不安全。生产化我会改成 MPSC ring，或者每个 source core 到 target core 一条 SPSC，再由目标轮询多个 inbox。

### 10.7 io_uring 的 `user_data` 为什么能打 tag？

答：

> 因为指针通常按至少 8 字节对齐，低 3 位为 0，所以可以把低 3 位拿来保存类型 tag。poll 时 `tag = ud & TAG_MASK`，`payload = ud & ~TAG_MASK`。风险是必须确保所有 payload 都满足对齐，否则会破坏地址。

### 10.8 Direct FD 有什么收益和代价？

答：

> 收益是减少 fd table 查找，适合高频 I/O。代价是要管理固定文件表容量，accept/close 都要走 direct fd 语义，和普通 fd 混用时容易出错，也要处理表满的问题。

### 10.9 Provided Buffer Ring 的难点是什么？

答：

> 难点是 buffer 生命周期。recv 完成后必须从 CQE flags 取 buffer id，用完归还；如果提前返回、异常、半包处理失败，buffer 可能泄漏。生产化我会把 buffer 封装成 RAII 对象，析构自动归还。

### 10.10 multishot recv 的坑是什么？

答：

> 一个 SQE 可以产生多个 CQE，但要看 `IORING_CQE_F_MORE` 判断是否还会继续。如果没有 MORE，就要重新提交。还要处理连接关闭、buffer 耗尽、错误码，以及 op 对象生命周期必须覆盖所有 CQE。

### 10.11 协程取消怎么做？

答：

> 当前 `SafeIoAwaiter` 只是析构时标记 cancelled，完成事件来了就不恢复协程，但这不等于取消内核 I/O。真正生产化需要提交 cancel 请求，或者让连接关闭路径保证内核完成后清理资源，避免 fd/buffer 泄漏。

### 10.12 你的 `Task` 和普通 future 有什么区别？

答：

> `Task` 是 coroutine awaitable，本身不创建线程；它通过 `co_await` 和 Scheduler 恢复执行。`std::future` 更偏线程间结果同步，等待通常会阻塞线程，不适合高并发 I/O 事件循环。

### 10.13 为什么 `DetachedTask` 要手动 schedule？

答：

> 它的 `initial_suspend` 是 `suspend_always`，创建后不会自动运行，需要把 `coro_` 交给 Scheduler。这样可以明确控制任务在哪个 Scheduler 上启动。

### 10.14 为什么不用 work stealing？

答：

> 当前项目强调每核本地性和 io_uring 亲和性，先保证单核路径简单高效。work stealing 可以改善负载倾斜，但会引入跨核同步、cache miss、任务亲和性和 I/O 所属 ring 的问题。生产化可以对纯 CPU 任务 stealing，对绑定连接的 I/O 任务保持亲和。

### 10.15 时间轮和小根堆怎么取舍？

答：

> 小根堆实现简单，适合 timer 数量不大或精确排序场景，插入删除 O(logN)。时间轮适合大量超时，插入接近 O(1)，但精度和级联逻辑复杂。网络长连接里大量心跳 timeout 更适合时间轮。

### 10.16 如果面试官质疑“你这个不能生产用”，怎么答？

答：

> 我会承认它现在是高性能 runtime 原型，不是完整生产框架。它验证了协程调度、io_uring 完成事件恢复、Direct FD、buffer ring、跨核唤醒这些关键链路。生产化我会优先补资源生命周期、取消语义、背压、MPSC 队列、可观测性和灰度退出。

### 10.17 企业微信这种后台为什么关心背压？

答：

> 因为消息洪峰时不能无限接收、无限分配、无限排队。要在连接级、用户级、队列级、I/O buffer 级做限制。比如 buffer ring 耗尽时暂停读，发送队列过长时限流或断开低优先级连接，避免局部过载拖垮整个 worker。

### 10.18 怎么保证消息有序？

答：

> 如果同一个连接始终绑定同一个 Scheduler，并且该连接上的读写状态机串行推进，就容易保证连接内顺序。跨连接或跨用户消息顺序要靠业务层 sequence、会话分片和幂等去保证。

### 10.19 如果一个协程 CPU 计算很久怎么办？

答：

> 当前框架没有抢占，会阻塞所在 Scheduler 的 I/O 处理。改进方式是规定协程必须主动 yield，或者引入 cooperative budget，执行超过预算就重新入队；CPU 密集任务可以投递到专门线程池。

### 10.20 你怎么压测？

答：

> 项目里有综合测试和 benchmark：基础测试验证协程返回/异常、when_all、CoMutex、Event；综合测试验证 SPSC、序列化、Direct FD、buffer ring echo；benchmark 模拟大量协程创建和多连接 ping-pong。真正面向生产还需要 wrk/自研压测、perf 火焰图、p99 延迟、内存占用、连接数、丢包率。

### 10.21 如果 io_uring 不可用怎么办？

答：

> 当前项目强依赖 Linux io_uring，不做兼容。工程化可以抽象 `IoEngine` 接口，Linux 新内核用 io_uring，老内核 fallback 到 epoll，但会损失 Direct FD 和 buffer ring 能力。

### 10.22 你为什么设置 CPU affinity 和 NUMA affinity？

答：

> 因为这个框架走的是每核调度模型。线程固定在某个 CPU 上可以提高 cache locality，减少调度迁移。mimalloc heap 设置 NUMA affinity 是为了尽量让该线程分配的内存在本地 NUMA 节点，减少跨 NUMA 访问成本。

### 10.23 你这个锁是公平的吗？

答：

> 等待者入队时是栈式挂到原子状态里，unlock 时会把等待链表反转，再从头恢复，所以目标上接近 FIFO。但当前实现还比较原型化，严格公平性和等待节点生命周期还需要更系统的测试。

### 10.24 为什么 `Event` 只支持一个等待者？

答：

> 当前 `Event` 的 state 只保存一个 coroutine address，所以它是轻量 one-shot event，不是 broadcast event。这样实现简单、开销低，但功能有限。生产化可以改成等待队列，支持多个 waiter 和 reset。

### 10.25 你的 `when_all` 异常怎么处理？

答：

> 当前 `Task` 本身会保存异常并在 `await_resume` 重新抛出，但 `when_all` helper 的异常路径还比较粗糙，存在 terminate 的情况。生产化要做异常聚合，或者一个任务失败时取消其他任务，并把错误返回给上层。

### 10.26 如果 buffer ring 耗尽怎么办？

答：

> 目前 benchmark 里对 `-ENOBUFS` 做了简单 sleep 重试，但这不是完整背压。更好的做法是暂停该连接读事件，等 buffer 归还后再恢复，或者按连接/用户限流，避免 buffer 被少数连接耗尽。

### 10.27 你为什么没有直接用成熟框架？

答：

> 如果做生产项目，我会优先考虑成熟框架。但这个项目的目的就是理解底层机制：协程 promise 怎么写，I/O 完成怎么恢复协程，跨核怎么唤醒，buffer 生命周期怎么管理。自己实现一遍能让我在使用成熟框架时也知道它底层的取舍和风险。

### 10.28 如果让你设计企业微信长连接网关，你会怎么用这些思想？

答：

> 我会按连接做分片，每个 worker 维护自己的连接集合和事件循环；连接心跳用时间轮；消息投递通过跨 worker 队列和唤醒机制；发送队列做背压；连接状态和用户状态通过分片存储保证局部有序；全局层面做限流、熔断、metrics 和灰度发布。NovaIO 提供的是这套架构的底层 runtime 思路。

## 11. 刁钻追问与高质量回答

### 11.1 你说 Direct FD 性能好，有数据吗？

建议回答：

> 当前项目做了功能验证和简单 benchmark，但我不会夸大说已经证明 Direct FD 一定更快。理论上它减少 fd table 查找，适合高频 I/O；但真实收益要用同等条件下的 epoll、普通 io_uring fd、Direct FD 做 A/B 压测，看 p50/p99、CPU cycles、syscall 次数和 cache miss。我后续会补这类基准。

### 11.2 你的 `io_uring_get_sqe()` 没判空，满了怎么办？

建议回答：

> 这是当前实现的不足。SQ 满时 `io_uring_get_sqe()` 会返回空。生产化应该先 submit 已有 SQE，再重试；仍失败则把当前任务挂到等待队列，等 SQE 资源恢复后继续，或者触发背压。

### 11.3 如果 `send_direct` 只发送了一部分怎么办？

建议回答：

> 示例里有些地方只判断了负数，benchmark 里对 partial send 做了循环补发。生产化应该统一封装 `async_write_all`，直到所有 buffer 发完或发生错误，避免业务层到处重复处理 partial write。

### 11.4 `SafeIoAwaiter` 析构标记 cancelled 会不会有竞态？

建议回答：

> 有这个风险。当前模型依赖协程和 CQE 处理都在同一 Scheduler 上时比较安全，但跨线程取消和内核完成并发时，仅靠普通 bool 不够严谨。生产化要用更明确的状态机、原子状态和真正的 io_uring cancel 请求。

### 11.5 你的 coroutine handle 生命周期安全吗？

建议回答：

> 基础 Task 链路是通过 RAII destroy 协程帧，I/O awaiter 完成后 delete `IoContext`。但 `DetachedTask` 和 multishot 回调对象生命周期需要业务保证，这也是原型项目的风险。生产化应引入连接对象持有协程、引用计数或 intrusive lifecycle，避免 handle 悬挂。

### 11.6 为什么不用 mutex 保护全局队列，简单很多？

建议回答：

> 可以用 mutex，会更简单，但高并发调度路径上锁竞争会影响延迟稳定性。我的设计用每核本地队列减少共享状态，跨核只在必要时通过 inbox 和 msg_ring 通信。这是用复杂度换低竞争。

### 11.7 你的时间轮 tick 每毫秒推进，如果线程卡住怎么办？

建议回答：

> Scheduler 每轮用 `current_time_ms()` 和上次 tick 对比，如果中间跳过了多个 ms，会循环补 tick。但如果线程卡住很久，会一次性处理大量过期 timer，引发延迟尖刺。生产化可以限制单轮 timer 处理预算，把剩余 timer 分批处理。

### 11.8 C++ 协程是不是一定比线程快？

建议回答：

> 不是。协程优势在 I/O 密集场景，它避免大量线程阻塞和上下文切换，让单线程复用大量连接。但 CPU 密集任务里协程不会自动并行，甚至可能阻塞事件循环。要根据任务类型选择模型。

### 11.9 为什么你用 C++26？

建议回答：

> 当前 CMake 设置了 C++26 和较新的 Clang，主要是为了使用较新的编译器和标准库能力，并配合 coroutine 优化属性。但项目核心依赖的是 C++20 coroutine。工程化时可以评估降到 C++20，提高可移植性。

### 11.10 如果线上 p99 延迟突然升高，你怎么排查？

建议回答：

> 我会先看每个 Scheduler 的任务队列长度、CQE 处理数量、timer 数量、buffer ring 可用数、SQE 使用率、active connections。再用 perf/flamegraph 看 CPU 热点，检查是否某个协程长时间运行、某个 core 负载倾斜、buffer 耗尽导致重试、发送队列积压或系统调用错误增多。

## 12. 推荐收尾话术

可以这样结束项目介绍：

> 这个项目我最核心的收获是，我不只是会写协程业务代码，而是理解协程 runtime 怎么和操作系统 I/O 完成机制对接。它现在还不是生产框架，但关键路径已经打通了：每核调度、io_uring、协程恢复、跨核唤醒、时间轮和内存优化。后续我会按成熟框架的方向补生命周期、背压、取消、可观测性和工程化。
