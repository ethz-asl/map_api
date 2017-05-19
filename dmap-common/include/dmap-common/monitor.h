#ifndef DMAP_COMMON_MONITOR_H_
#define DMAP_COMMON_MONITOR_H_

#include <mutex>

#include <glog/logging.h>

namespace dmap_common {

// Recommend to use this e.g. for member variables that must be protected by a
// mutex; this will enforce mutex lock in order to access.
template <typename MonitoredType>
class Monitor {
 public:
  Monitor() {}

  explicit Monitor(MonitoredType object) : object_(object) {}

  explicit Monitor(const Monitor<MonitoredType>& other)
      : object_(other.object_) {}

  class ThreadSafeAccess {
   public:
    ThreadSafeAccess(MonitoredType* object, std::mutex* mutex)
        : object_(CHECK_NOTNULL(object)), lock_(*CHECK_NOTNULL(mutex)) {}

    MonitoredType* operator->() { return object_; }

    MonitoredType& operator*() { return *object_; }

   private:
    MonitoredType* object_;
    std::unique_lock<std::mutex> lock_;
  };

  ThreadSafeAccess get() { return ThreadSafeAccess(&object_, &mutex_); }
  ThreadSafeAccess* allocatedAccess() {
    return new ThreadSafeAccess(&object_, &mutex_);
  }

 private:
  MonitoredType object_;
  std::mutex mutex_;
};

}  // namespace dmap_common

#endif  // DMAP_COMMON_MONITOR_H_
