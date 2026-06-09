#pragma once
#include "novaio/scheduler.hpp"
#include <vector>
#include <memory>
#include <thread>

namespace novaio {

class Runtime {
public:
    static Runtime& get() {
        static Runtime instance;
        return instance;
    }

    std::atomic<bool> is_running_{true};
    void start(size_t num_cores);
    void stop();

    
    void dispatch_on(size_t core_id, UniqueTask task);
    void notify_target_ring(int target_fd, uint64_t msg_data);
    Scheduler* get_scheduler(size_t id) { return schedulers_[id].get(); }
    size_t get_core_count() const { return schedulers_.size(); }

private:
    Runtime() = default;
    ~Runtime() = default;
    std::vector<std::unique_ptr<Scheduler>> schedulers_;
    std::vector<std::thread> workers_;
};

} // namespace novaio