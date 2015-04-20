#include "map-api/workspace.h"

namespace map_api {

Workspace::Workspace(const std::initializer_list<NetTable*>& table_blacklist,
                     const std::initializer_list<NetTable*>& table_whitelist)
    : table_blacklist_(table_blacklist), table_whitelist_(table_whitelist) {}

bool Workspace::contains(NetTable* table, const common::Id& id) const {
  CHECK_NOTNULL(table);
  // Fast default case
  if (table_blacklist_.empty() && table_whitelist_.empty() &&
      chunk_blacklist_.empty() && chunk_whitelist_.empty()) {
    return true;
  }

  TrackeeMultimap::const_iterator black_submap = chunk_blacklist_.find(table);
  TrackeeMultimap::const_iterator white_submap = chunk_whitelist_.find(table);

  bool black_found = black_submap != chunk_blacklist_.end();
  bool white_found = white_submap != chunk_whitelist_.end();

  // False if table blacklisted or chunk blacklisted.
  if (table_blacklist_.find(table) != table_blacklist_.end() ||
      (black_found &&
       black_submap->second.find(id) != black_submap->second.end())) {
    return false;
  }

  bool allowed_by_table_whitelist =
      table_whitelist_.empty() ||
      (table_whitelist_.find(table) != table_whitelist_.end());

  bool allowed_by_chunk_whitelist =
      !white_found || white_submap->second.empty() ||
      (white_submap->second.find(id) != white_submap->second.end());

  return allowed_by_chunk_whitelist && allowed_by_table_whitelist;
}

void Workspace::mergeRevisionTrackeesIntoBlacklist(const Revision& revision) {
  TrackeeMultimap trackees;
  trackees.deserialize(revision);
  chunk_blacklist_.merge(trackees);
}

void Workspace::mergeRevisionTrackeesIntoWhitelist(const Revision& revision) {
  TrackeeMultimap trackees;
  trackees.deserialize(revision);
  chunk_whitelist_.merge(trackees);
}

}  // namespace map_api
