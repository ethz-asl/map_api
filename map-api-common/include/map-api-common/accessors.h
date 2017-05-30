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

#ifndef MAP_API_COMMON_ACCESSORS_H_
#define MAP_API_COMMON_ACCESSORS_H_

#include <functional>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "map-api-common/aligned-allocation.h"

/// Returns a const reference to the value for the specified key in an std::unordered_map.
/// Fails hard if the keys does not exist.
namespace map_api_common {
template <typename KeyType, typename ValueType, template <typename> class Hash,
          template <typename> class Comparator,
          template <typename> class Allocator>
inline const ValueType& getChecked(
    const std::unordered_map<
        KeyType, ValueType, Hash<KeyType>, Comparator<KeyType>,
        Allocator<std::pair<const KeyType, ValueType> > >& map,
    const KeyType& key) {
  typename std::unordered_map<
      KeyType, ValueType, std::hash<KeyType>, std::equal_to<KeyType>,
      Allocator<std::pair<const KeyType, ValueType> > >::const_iterator it =
      map.find(key);
  CHECK(it != map.end());
  return it->second;
}

/// Returns a non-const reference to the value for the specified key in an
/// std::unordered_map. Fails hard if the keys does not exist.
template <typename KeyType, typename ValueType, template <typename> class Hash,
          template <typename> class Comparator,
          template <typename> class Allocator>
inline ValueType& getChecked(
    std::unordered_map<KeyType, ValueType, Hash<KeyType>, Comparator<KeyType>,
                       Allocator<std::pair<const KeyType, ValueType> > >&
        map,  // NOLINT
    const KeyType& key) {
  typename std::unordered_map<
      KeyType, ValueType, std::hash<KeyType>, std::equal_to<KeyType>,
      Allocator<std::pair<const KeyType, ValueType> > >::iterator it =
      map.find(key);
  CHECK(it != map.end());
  return it->second;
}

}  // namespace map_api_common

#endif  // MAP_API_COMMON_ACCESSORS_H_
