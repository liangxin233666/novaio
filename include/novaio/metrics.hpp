#pragma once

#include <atomic>
#include <cstdint>
#include <vector>
#include <shared_mutex>
#include <mutex>
#include "novaio/concurrent_queues.hpp"

//#define NOVAIO_ENABLE_METRICS 1

namespace novaio {

#ifdef NOVAIO_ENABLE_METRICS
struct alignas(CACHE_LINE) TcmMetrics {
    uint64_t tasks_executed{0};
    uint64_t io_events_reaped{0};
    uint64_t inbox_messages_processed{0};
};
#else
struct TcmMetrics {};
#endif

extern thread_local TcmMetrics current_thread_metrics;

inline void metrics_record_task_executed() noexcept {
#ifdef NOVAIO_ENABLE_METRICS
    current_thread_metrics.tasks_executed++;
#endif
}

inline void metrics_record_io_event() noexcept {
#ifdef NOVAIO_ENABLE_METRICS
    current_thread_metrics.io_events_reaped++;
#endif
}

inline void metrics_record_inbox_processed() noexcept {
#ifdef NOVAIO_ENABLE_METRICS
    current_thread_metrics.inbox_messages_processed++;
#endif
}

class MetricsRegistry {
public:
    static MetricsRegistry& get() {
        static MetricsRegistry instance;
        return instance;
    }

    inline void register_thread(TcmMetrics* metrics) {
#ifdef NOVAIO_ENABLE_METRICS
        std::unique_lock lock(rw_mutex_);
        thread_metrics_.push_back(metrics);
#endif
    }

    inline void unregister_thread(TcmMetrics* metrics) {
#ifdef NOVAIO_ENABLE_METRICS
        std::unique_lock lock(rw_mutex_);
        for (auto it = thread_metrics_.begin(); it != thread_metrics_.end(); ++it) {
            if (*it == metrics) {
                thread_metrics_.erase(it);
                break;
            }
        }
#endif
    }

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

private:
    MetricsRegistry() = default;
    ~MetricsRegistry() = default;

#ifdef NOVAIO_ENABLE_METRICS
    mutable std::shared_mutex rw_mutex_;
    std::vector<TcmMetrics*> thread_metrics_;
#endif
};

} // namespace novaio