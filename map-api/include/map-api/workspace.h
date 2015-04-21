#ifndef MAP_API_WORKSPACE_H_
#define MAP_API_WORKSPACE_H_

#include <initializer_list>
#include <unordered_set>

#include "map-api/trackee-multimap.h"

namespace map_api {
class Chunk;
class Revision;

// See contains() implementation for precedence details.
// If a whitelist is empty, all items covered are considered whitelisted.
// Workspaces are only used to restrict reading operations, not writing
// operations.
class Workspace {
 public:
  Workspace(const std::initializer_list<NetTable*>& table_blacklist,
            const std::initializer_list<NetTable*>& table_whitelist);
  Workspace();

  bool contains(NetTable* table, const common::Id& chunk_id) const;
  bool contains(NetTable* table) const;

  // The chunk of the revision itself is also merged.
  void mergeRevisionTrackeesIntoBlacklist(const Revision& revision,
                                          NetTable* tracker_table);
  void mergeRevisionTrackeesIntoWhitelist(const Revision& revision,
                                          NetTable* tracker_table);

  class TableInterface {
   public:
    TableInterface(const Workspace& workspace, NetTable* table);
    bool contains(const common::Id& chunk_id) const;
    void forEachChunk(const std::function<void(const Chunk& chunk)>& action)
        const;

   private:
    const Workspace& workspace_;
    NetTable* table_;
  };

 private:
  std::unordered_set<NetTable*> table_blacklist_;
  std::unordered_set<NetTable*> table_whitelist_;
  TrackeeMultimap chunk_blacklist_;
  TrackeeMultimap chunk_whitelist_;
};

}  // namespace map_api

#endif  // MAP_API_WORKSPACE_H_
