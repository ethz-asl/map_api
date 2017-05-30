// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

#ifndef DMAP_COMMON_MONITOR_H_
#define DMAP_COMMON_MONITOR_H_

#include <mutex>

#include <glog/logging.h>

namespace map_api_common {

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

}  // namespace map_api_common

#endif  // DMAP_COMMON_MONITOR_H_
