#ifndef MAP_API_WORKSPACE_H_
#define MAP_API_WORKSPACE_H_

#include <initializer_list>
#include <unordered_set>

#include "map-api/trackee-multimap.h"

namespace map_api {
class Revision;

// Precedence: Chunk_black > Table_black > Chunk_white > Table_white, see
// contains() implementation for details.
// If a whitelist is empty, all items covered are considered whitelisted.
class Workspace {
 public:
  Workspace(const std::initializer_list<NetTable*>& table_blacklist,
            const std::initializer_list<NetTable*>& table_whitelist);

  bool contains(NetTable* table, const common::Id& chunk_id) const;

  // The chunk of the revision itself is also merged.
  void mergeRevisionTrackeesIntoBlacklist(const Revision& revision);
  void mergeRevisionTrackeesIntoWhitelist(const Revision& revision);

 private:
  std::unordered_set<NetTable*> table_blacklist_;
  std::unordered_set<NetTable*> table_whitelist_;
  TrackeeMultimap chunk_blacklist_;
  TrackeeMultimap chunk_whitelist_;
};

}  // namespace map_api

#endif  // MAP_API_WORKSPACE_H_
