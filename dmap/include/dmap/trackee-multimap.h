#ifndef DMAP_TRACKEE_MULTIMAP_H_
#define DMAP_TRACKEE_MULTIMAP_H_

#include <unordered_map>

#include <multiagent-mapping-common/unique-id.h>

namespace dmap {
class NetTable;
class Revision;

namespace proto {
class Revision;
}  // namespace proto

class TrackeeMultimap
    : public std::unordered_map<NetTable*, std::unordered_set<common::Id>> {
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

}  // namespace dmap

#endif  // DMAP_TRACKEE_MULTIMAP_H_
