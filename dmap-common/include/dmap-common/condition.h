#ifndef DMAP_COMMON_CONDITION_H_
#define DMAP_COMMON_CONDITION_H_

#include <condition_variable>
#include <mutex>

namespace dmap_common {

class Condition {
 public:
  Condition();
  void wait() const;
  void notify();

 private:
  bool bool_;
  mutable std::mutex mutex_;
  mutable std::condition_variable cv_;
};

}  // namespace dmap_common

#endif  // DMAP_COMMON_CONDITION_H_
