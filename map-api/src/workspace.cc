#include "map-api/workspace.h"

#include <sstream>  // NOLINT

#include "../include/map-api/legacy-chunk.h"
#include "map-api/net-table.h"

namespace map_api {

Workspace::Workspace(const std::initializer_list<NetTable*>& table_blacklist,
                     const std::initializer_list<NetTable*>& table_whitelist)
    : table_blacklist_(table_blacklist), table_whitelist_(table_whitelist) {}
Workspace::Workspace() : Workspace({}, {}) {}

// "table" is an input argument.
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

bool Workspace::contains(NetTable* table) const {
  if (table_blacklist_.empty() && table_whitelist_.empty()) {
    return true;
  }
  if (table_blacklist_.find(table) != table_blacklist_.end()) {
    return false;
  }
  if (table_whitelist_.empty() ||
      (table_whitelist_.find(table) != table_whitelist_.end())) {
    return true;
  } else {
    return false;
  }
}

void Workspace::mergeRevisionTrackeesIntoBlacklist(const Revision& revision,
                                                   NetTable* tracker_table) {
  CHECK_NOTNULL(tracker_table);
  TrackeeMultimap trackees;
  trackees.deserialize(revision);
  chunk_blacklist_.merge(trackees);
  chunk_blacklist_[tracker_table].emplace(revision.getChunkId());
}

void Workspace::mergeRevisionTrackeesIntoWhitelist(const Revision& revision,
                                                   NetTable* tracker_table) {
  CHECK_NOTNULL(tracker_table);
  TrackeeMultimap trackees;
  trackees.deserialize(revision);
  chunk_whitelist_.merge(trackees);
  chunk_whitelist_[tracker_table].emplace(revision.getChunkId());
}

Workspace::TableInterface::TableInterface(const Workspace& workspace,
                                          NetTable* table)
    : workspace_(workspace), table_(table) {}

bool Workspace::TableInterface::contains(const common::Id& chunk_id) const {
  return workspace_.contains(table_, chunk_id);
}

void Workspace::TableInterface::forEachChunk(
    const std::function<void(const ChunkBase& chunk)>& action) const {
  // Check if table is blacklisted.
  if (!workspace_.contains(table_)) {
    return;
  }

  TrackeeMultimap::const_iterator black_submap =
      workspace_.chunk_blacklist_.find(table_);
  TrackeeMultimap::const_iterator white_submap =
      workspace_.chunk_whitelist_.find(table_);
  bool black_found = black_submap != workspace_.chunk_blacklist_.end();
  bool white_found = white_submap != workspace_.chunk_whitelist_.end();

  std::function<void(const ChunkBase & chunk)> chunk_functional;
  // Use a faster functional if the blacklist is empty.
  if (!black_found || black_submap->second.empty()) {
    chunk_functional = [action](const ChunkBase& chunk) {
      action(chunk);
    };      // NOLINT
  } else {  // Otherwise we need to filter using the blacklist.
    chunk_functional = [&, this](const ChunkBase& chunk) {
      if (black_submap->second.find(chunk.id()) == black_submap->second.end()) {
        action(chunk);
      }
    };
  }

  // No whitelist: Use all active chunks.
  if (!white_found || white_submap->second.empty()) {
    table_->forEachActiveChunk(chunk_functional);
  } else {
    for (const common::Id& chunk_id : white_submap->second) {
      chunk_functional(*table_->getChunk(chunk_id));
    }
  }
}

std::string Workspace::debugString() const {
  std::ostringstream ss;
  ss << "Blacklisted tables:\n";
  for (NetTable* table : table_blacklist_) {
    CHECK_NOTNULL(table);
    ss << "\t" << table->name() << "\n";
  }
  ss << "Whitelisted tables:\n";
  for (NetTable* table : table_whitelist_) {
    CHECK_NOTNULL(table);
    ss << "\t" << table->name() << "\n";
  }

  ss << "Blacklisted chunks:\n";
  for (TrackeeMultimap::value_type table_chunk : chunk_blacklist_) {
    CHECK_NOTNULL(table_chunk.first);
    ss << "\t" << table_chunk.first->name() << ": " << table_chunk.second.size()
       << " chunks.\n";
  }
  ss << "Whitelisted chunks:\n";
  for (TrackeeMultimap::value_type table_chunk : chunk_whitelist_) {
    CHECK_NOTNULL(table_chunk.first);
    ss << "\t" << table_chunk.first->name() << ": " << table_chunk.second.size()
       << " chunks.\n";
  }

  return ss.str();
}

}  // namespace map_api
