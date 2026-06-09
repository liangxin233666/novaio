#pragma once

#include <atomic>
#include <coroutine>
#include <cstddef>
#include <stdexcept>
#include <new> 

namespace novaio {

#if defined(__cpp_lib_hardware_interference_size)
    constexpr std::size_t CACHE_LINE = std::hardware_destructive_interference_size;
#else
    constexpr std::size_t CACHE_LINE = 64;
#endif

class LocalQueue {
public:
    explicit LocalQueue(std::size_t capacity = 65536) 
        : capacity_(capacity), mask_(capacity - 1) {
        if ((capacity & (capacity - 1)) != 0) throw std::invalid_argument("Power of 2 required");
        buffer_ = new std::coroutine_handle<>[capacity_];
    }
    
    ~LocalQueue() { delete[] buffer_; }

    
    void push(std::coroutine_handle<> item) noexcept {
        buffer_[tail_++ & mask_] = item;
    }

    std::coroutine_handle<> pop() noexcept {
        if (head_ == tail_) return nullptr;
        return buffer_[head_++ & mask_];
    }

    bool empty() const noexcept { return head_ == tail_; }

private:
    std::size_t capacity_, mask_;
    uint32_t head_{0};
    uint32_t tail_{0};
    std::coroutine_handle<>* buffer_{nullptr};
};


template <typename T>
class SpscQueue {
public:
    explicit SpscQueue(std::size_t capacity = 65536) 
        : capacity_(capacity), mask_(capacity - 1) {
        if ((capacity & (capacity - 1)) != 0) throw std::invalid_argument("Power of 2 required");
        buffer_ = static_cast<T*>(::operator new[](capacity_ * sizeof(T)));
    }

    ~SpscQueue() {
        while (head_.load(std::memory_order_relaxed) != tail_.load(std::memory_order_relaxed)) {
            std::size_t current_head = head_.load(std::memory_order_relaxed);
            buffer_[current_head & mask_].~T();
            head_.store(current_head + 1, std::memory_order_relaxed);
        }
        ::operator delete[](buffer_);
    }

    bool push(T item) {
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        std::size_t next_tail = tail + 1;
        if (next_tail - head_.load(std::memory_order_acquire) > capacity_) {
            return false; // Queue Full
        }
        new (&buffer_[tail & mask_]) T(std::move(item));
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    bool pop(T& item_out) {
        std::size_t head = head_.load(std::memory_order_relaxed);
        if (head == tail_.load(std::memory_order_acquire)) {
            return false; // Queue Empty
        }
        item_out = std::move(buffer_[head & mask_]);
        buffer_[head & mask_].~T();
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

private:
    std::size_t capacity_, mask_;
    
   
    alignas(CACHE_LINE) std::atomic<std::size_t> tail_{0};
    alignas(CACHE_LINE) std::atomic<std::size_t> head_{0};
    T* buffer_{nullptr};
};

} // namespace novaio