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

#include <map-api/logical-time.h>
#include <glog/logging.h>

#include <map-api/peer-id.h>

namespace map_api {

uint64_t LogicalTime::current_ = 1u;
std::mutex LogicalTime::current_mutex_;

LogicalTime::LogicalTime() : value_(0u) {}

LogicalTime::LogicalTime(uint64_t serialized) : value_(serialized) {}

bool LogicalTime::isValid() const { return (value_ > 0u); }

LogicalTime LogicalTime::sample() {
  LogicalTime time;
  std::lock_guard<std::mutex> lock(current_mutex_);
  time.value_ = current_;
  ++current_;
  return time;
}

uint64_t LogicalTime::serialize() const { return value_; }

void LogicalTime::synchronize(const LogicalTime& other_time) {
  std::lock_guard<std::mutex> lock(current_mutex_);
  if (other_time.value_ >= current_) {
    current_ = other_time.value_ + 1u;
  }
  VLOG(3) << "Logical time at " << PeerId::self() << " synced to " << current_;
}

LogicalTime LogicalTime::justBefore() const { return LogicalTime(value_ - 1); }

}  // namespace map_api
