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

#include "map-api-common/internal/unique-id.h"

#include <atomic>
#include <chrono>

namespace map_api_common {
namespace internal {

void generateUnique128BitHash(uint64_t hash[2]) {
  static_assert(sizeof(size_t) == sizeof(uint64_t),
                "Please adapt the below to your non-64-bit system.");

  static std::atomic<int> counter;
  hash[0] =
      UniqueIdHashSeed::instance().seed() ^ std::hash<int>()(
          std::chrono::high_resolution_clock::now().time_since_epoch().count());
  // Increment must happen here, otherwise the sampling is not atomic.
  hash[1] = std::hash<int>()(++counter);
}
}  // namespace internal
}  // namespace map_api_common
