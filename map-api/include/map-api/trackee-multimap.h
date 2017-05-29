#ifndef DMAP_TRACKEE_MULTIMAP_H_
#define DMAP_TRACKEE_MULTIMAP_H_

#include <unordered_map>

#include <map-api-common/unique-id.h>

namespace map_api {
class NetTable;
class Revision;

namespace proto {
class Revision;
}  // namespace proto

class TrackeeMultimap
    : public std::unordered_map<NetTable*, std::unordered_set<map_api_common::Id>> {
 public:
  void deserialize(const proto::Revision& proto);
  void deserialize(const Revision& revision);
  void serialize(proto::Revision* proto) const;
  void serialize(Revision* revision) const;

  // Returns true if the merge results in a change, false otherwise.
  bool merge(const TrackeeMultimap& other);

  bool hasOverlap(const TrackeeMultimap& other) const;

  bool isSameVerbose(const TrackeeMultimap& other) const;
};

}  // namespace map_api

#endif  // DMAP_TRACKEE_MULTIMAP_H_
