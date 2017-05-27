#ifndef DMAP_COMMON_ACCESSORS_H_
#define DMAP_COMMON_ACCESSORS_H_

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

#endif  // DMAP_COMMON_ACCESSORS_H_
