#pragma once
#include <cstdint>
#include <utility>
#include "novaio/scheduler.hpp"

namespace novaio {

class IoBuffer {
public:
    IoBuffer() = default;
    IoBuffer(uint16_t id, void* data, Scheduler* sched)
        : id_(id), data_(data), sched_(sched) {}

    IoBuffer(IoBuffer&& other) noexcept
        : id_(other.id_), data_(other.data_), sched_(other.sched_) {
        other.data_ = nullptr;
    }

    IoBuffer& operator=(IoBuffer&& other) noexcept {
        if (this != &other) {
            release();
            id_ = other.id_;
            data_ = other.data_;
            sched_ = other.sched_;
            other.data_ = nullptr;
        }
        return *this;
    }

    ~IoBuffer() { release(); }

    void* data() const noexcept { return data_; }
    uint32_t size() const noexcept { return BUF_BLOCK_SIZE; } // 从 io_engine.hpp 获取
    explicit operator bool() const noexcept { return data_ != nullptr; }

private:
    void release() {
        if (data_ && sched_) {
            sched_->io_engine().return_buffer(id_);
            data_ = nullptr;
        }
    }

    uint16_t id_{0};
    void* data_{nullptr};
    Scheduler* sched_{nullptr};
};

} // namespace novaio