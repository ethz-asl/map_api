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

#ifndef DMAP_COMMON_INTERNAL_UNIQUE_ID_H_  // NOLINT
#define DMAP_COMMON_INTERNAL_UNIQUE_ID_H_  // NOLINT

#include <atomic>
#include <string>

#include "./id.pb.h"

namespace map_api {
class Hub;
}  // namespace map_api
namespace map_api_common {
namespace internal {
class UniqueIdHashSeed {
 public:
  class Key {
    friend map_api::Hub;
    Key() { }
  };

  UniqueIdHashSeed() : seed_(31u) { }
  static UniqueIdHashSeed& instance() {
    static UniqueIdHashSeed instance;
    return instance;
  }

  void saltSeed(const Key&, size_t salt) {
    seed_ ^= salt;
  }

  size_t seed() const {
    return seed_;
  }

 private:
  std::atomic<size_t> seed_;
};

void generateUnique128BitHash(uint64_t hash[2]);
}  // namespace internal
}  // namespace map_api_common

#endif  // DMAP_COMMON_INTERNAL_UNIQUE_ID_H_  NOLINT
