#include "novaio/runtime.hpp"
#include <stdexcept>

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
        schedulers_[core_id]->push_inbox(std::move(task));
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

} // namespace novaio