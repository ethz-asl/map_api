#ifndef MAP_API_TRACKEE_MULTIMAP_H_
#define MAP_API_TRACKEE_MULTIMAP_H_

#include <unordered_map>

#include "map-api/unique-id.h"

namespace map_api {
class NetTable;

namespace proto {
class Revision;
}  // namespace proto

class TrackeeMultimap
    : public std::unordered_map<NetTable*, std::unordered_set<Id>> {
 public:
  void deserialize(const proto::Revision& proto);
  void serialize(proto::Revision* proto) const;

  void merge(const TrackeeMultimap& other);

  bool hasOverlap(const TrackeeMultimap& other) const;

  bool isSameVerbose(const TrackeeMultimap& other) const;
};

}  // namespace map_api

#endif  // MAP_API_TRACKEE_MULTIMAP_H_
