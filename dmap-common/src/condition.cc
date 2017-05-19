#include "dmap-common/condition.h"

namespace dmap_common {

Condition::Condition() : bool_(false) {}

void Condition::wait() const {
  std::unique_lock<std::mutex> lock(mutex_);
  cv_.wait(lock, [this] { return bool_; });
}

void Condition::notify() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    bool_ = true;
  }
  cv_.notify_all();
}

}  // namespace dmap_common
