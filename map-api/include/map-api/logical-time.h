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

#ifndef DMAP_LOGICAL_TIME_H_
#define DMAP_LOGICAL_TIME_H_

#include <cstdint>
#include <iostream>  // NOLINT
#include <mutex>

namespace map_api {
class LogicalTime {
 public:
  /**
   * Invalid time
   */
  LogicalTime();
  /**
   * To deserialize from database.
   */
  explicit LogicalTime(uint64_t serialized);

  bool isValid() const;
  /**
   * Returns a current logical time and advances the value of the clock by one
   */
  static LogicalTime sample();

  uint64_t serialize() const;
  /**
   * If other_time exceeds or equals current_, current_ is advanced to
   * other_time + 1
   */
  static void synchronize(const LogicalTime& other_time);

  LogicalTime justBefore() const;

  inline bool operator<(const LogicalTime& other) const;
  inline bool operator<=(const LogicalTime& other) const;
  inline bool operator>(const LogicalTime& other) const;
  inline bool operator>=(const LogicalTime& other) const;
  inline bool operator==(const LogicalTime& other) const;
  inline bool operator!=(const LogicalTime& other) const;

 private:
  uint64_t value_;
  static uint64_t current_;
  static std::mutex current_mutex_;
};

}  //  namespace map_api

namespace std {
inline ostream& operator<<(ostream& out, const map_api::LogicalTime& time) {
  out << "Logical time(" << time.serialize() << ")";
  return out;
}

template <>
struct hash<map_api::LogicalTime> {
  inline size_t operator()(const map_api::LogicalTime& time) const {
    return std::hash<uint64_t>()(time.serialize());
  }
};
}  //  namespace std

#include "./logical-time-inl.h"

#endif  // DMAP_LOGICAL_TIME_H_
