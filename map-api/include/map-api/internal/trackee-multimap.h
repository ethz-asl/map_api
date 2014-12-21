#ifndef MAP_API_INTERNAL_TRACKEE_MULTIMAP_H_
#define MAP_API_INTERNAL_TRACKEE_MULTIMAP_H_

#include <unordered_map>

#include "map-api/unique-id.h"

namespace map_api {
class NetTable;

namespace proto {
class Revision;
}  // namespace proto

class TrackeeMultimap : public std::unordered_multimap<NetTable*, Id> {
 public:
  void deserialize(const proto::Revision& proto);
  void serialize(proto::Revision* proto) const;
};

}  // namespace map_api

#endif  // MAP_API_INTERNAL_TRACKEE_MULTIMAP_H_
