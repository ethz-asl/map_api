#include "map-api/internal/delta-view.h"

#include <map-api-common/unique-id.h>

#include "map-api/chunk-base.h"
#include "map-api/conflicts.h"
#include "map-api/net-table.h"

DECLARE_bool(map_api_blame_updates);

namespace map_api {
namespace internal {

DeltaView::DeltaView(const NetTable& table) : table_(table) {}

DeltaView::~DeltaView() {}

bool DeltaView::has(const map_api_common::Id& id) const {
  return (updates_.count(id) != 0u || insertions_.count(id) != 0u) &&
         removes_.count(id) == 0u;
}

std::shared_ptr<const Revision> DeltaView::get(const map_api_common::Id& id) const {
  CHECK_EQ(removes_.count(id), 0u);
  UpdateMap::const_iterator found = updates_.find(id);
  if (found != updates_.end()) {
    return std::const_pointer_cast<const Revision>(found->second);
  } else {
    InsertMap::const_iterator found = insertions_.find(id);
    CHECK(found != insertions_.end());
    return std::const_pointer_cast<const Revision>(found->second);
  }
}

void DeltaView::dump(ConstRevisionMap* result) const {
  CHECK_NOTNULL(result)->clear();
  for (const InsertMap::value_type& item : insertions_) {
    result->emplace(item);
  }
  for (const UpdateMap::value_type& item : updates_) {
    result->emplace(item);
  }
}

void DeltaView::getAvailableIds(std::unordered_set<map_api_common::Id>* result) const {
  CHECK_NOTNULL(result)->clear();
  for (const InsertMap::value_type& item : insertions_) {
    result->emplace(item.first);
  }
  for (const UpdateMap::value_type& item : updates_) {
    result->emplace(item.first);
  }
}

void DeltaView::discardKnownUpdates(UpdateTimes* update_times) const {
  LOG(FATAL) << "This function should never be called on a delta view!";
}

bool DeltaView::suppresses(const map_api_common::Id& id) const {
  return removes_.count(id) != 0u;
}

void DeltaView::insert(std::shared_ptr<Revision> revision) {
  map_api_common::Id id = revision->getId<map_api_common::Id>();
  CHECK(id.isValid());
  CHECK_EQ(updates_.count(id), 0u);
  CHECK_EQ(removes_.count(id), 0u);
  // Check emplacement, as insertions are mutually exclusive.
  CHECK(insertions_.emplace(id, revision).second);
}

void DeltaView::update(std::shared_ptr<Revision> revision) {
  map_api_common::Id id = revision->getId<map_api_common::Id>();
  CHECK(id.isValid());
  InsertMap::iterator corresponding_insertion = insertions_.find(id);
  if (corresponding_insertion != insertions_.end()) {
    // If this updates a revision added also in this transaction, the insertion
    // is replaced with the update, in order to ensure the setting of default
    // fields such as insert time and chunk id.
    corresponding_insertion->second = revision;
  } else {
    CHECK_EQ(removes_.count(id), 0u);
    // Assignment, as later updates supersede earlier ones.
    updates_[id] = revision;
  }
}

void DeltaView::remove(std::shared_ptr<Revision> revision) {
  const map_api_common::Id id = revision->getId<map_api_common::Id>();
  CHECK(id.isValid());

  UpdateMap::iterator corresponding_update = updates_.find(id);
  if (corresponding_update != updates_.end()) {
    updates_.erase(corresponding_update);
  }

  InsertMap::iterator corresponding_insertion = insertions_.find(id);
  if (corresponding_insertion != insertions_.end()) {
    insertions_.erase(corresponding_insertion);
  } else {
    CHECK(removes_.emplace(id, revision).second);
  }
}

bool DeltaView::getMutableUpdateEntry(
    const map_api_common::Id& id, std::shared_ptr<const Revision>** result) {
  CHECK_NOTNULL(result);
  // Is there already a corresponding entry in the update map?
  UpdateMap::iterator existing_entry = updates_.find(id);
  if (existing_entry != updates_.end()) {
    *result = reinterpret_cast<std::shared_ptr<const Revision>*>(
        &existing_entry->second);
    return true;
  }
  // Is there a corresponding entry in the insert map?
  InsertMap::iterator existing_insert_entry = insertions_.find(id);
  if (existing_insert_entry != insertions_.end()) {
    *result = reinterpret_cast<std::shared_ptr<const Revision>*>(
        &existing_insert_entry->second);
    return true;
  }

  return false;
}

bool DeltaView::hasConflictsAfterTryingToMerge(
    const std::unordered_map<map_api_common::Id, LogicalTime>& potential_conflicts,
    const ViewBase& original_view, const ViewBase& conflict_view) {
  return traverseConflicts(ConflictTraversalMode::kTryMergeOrBail,
                           potential_conflicts, original_view, conflict_view,
                           nullptr, nullptr);
}

void DeltaView::checkedCommitLocked(
    const LogicalTime& commit_time, ChunkBase* locked_chunk,
    std::unordered_map<map_api_common::Id, LogicalTime>* commit_history) {
  CHECK(CHECK_NOTNULL(locked_chunk)->isWriteLocked());
  // Don't clear, this may already contain history from previous commits!
  CHECK_NOTNULL(commit_history);

  insertions_.logCommitEvent(commit_time, commit_history);
  locked_chunk->bulkInsertLocked(insertions_, commit_time);

  if (FLAGS_map_api_blame_updates) {
    std::cout << "Updating " << updates_.size() << " items" << std::endl;
  }

  for (const std::pair<const map_api_common::Id, std::shared_ptr<Revision> >& item :
       updates_) {
    locked_chunk->updateLocked(commit_time, item.second);
  }

  for (const std::pair<const map_api_common::Id, std::shared_ptr<Revision> >& item :
       removes_) {
    locked_chunk->removeLocked(commit_time, item.second);
  }

  for (RevisionEventMap* map :
       std::vector<RevisionEventMap*>({&insertions_, &updates_, &removes_})) {
    map->logCommitEvent(commit_time, commit_history);
    map->clear();
  }
}

void DeltaView::prepareManualMerge(
    const std::unordered_map<map_api_common::Id, LogicalTime>& potential_conflicts,
    const ViewBase& original_view, const ViewBase& conflict_view,
    DeltaView* conflict_free_part, Conflicts* conflicts) {
  CHECK_NOTNULL(conflict_free_part);
  // Don't clear, this is shared across chunk transactions.
  CHECK_NOTNULL(conflicts);
  CHECK(!traverseConflicts(ConflictTraversalMode::kPrepareManualMerge,
                           potential_conflicts, original_view, conflict_view,
                           conflict_free_part, conflicts));
}

size_t DeltaView::numChanges() const {
  return insertions_.size() + updates_.size() + removes_.size();
}

void DeltaView::RevisionEventMap::logCommitEvent(
    const LogicalTime& commit_time,
    std::unordered_map<map_api_common::Id, LogicalTime>* commit_history) const {
  CHECK_NOTNULL(commit_history);  // Don't clear!
  for (const value_type& item : *this) {
    (*commit_history)[item.first] = commit_time;
  }
}

bool DeltaView::traverseConflicts(
    const ConflictTraversalMode mode,
    const std::unordered_map<map_api_common::Id, LogicalTime>& potential_conflicts,
    const ViewBase& original_view, const ViewBase& conflict_view,
    DeltaView* conflict_free_part, Conflicts* conflicts) {
  if (mode == ConflictTraversalMode::kPrepareManualMerge) {
    CHECK_NOTNULL(conflict_free_part);
    CHECK_NOTNULL(conflicts)->clear();
  }

  for (const InsertMap::value_type& item : insertions_) {
    if (potential_conflicts.count(item.first) != 0u) {
      if (mode == ConflictTraversalMode::kTryMergeOrBail) {
        VLOG(4) << "Tried to insert item " << item.first << " twice!";
        return true;
      } else {
        CHECK(mode == ConflictTraversalMode::kPrepareManualMerge);
        conflicts->push_back({conflict_view.get(item.first), item.second});
      }
    } else {
      if (mode == ConflictTraversalMode::kPrepareManualMerge) {
        conflict_free_part->insertions_.emplace(item);
      }
    }
  }

  for (UpdateMap::value_type& item : updates_) {
    if (potential_conflicts.count(item.first) != 0u) {
      if (mode == ConflictTraversalMode::kTryMergeOrBail) {
        VLOG(4) << "Update conflict!";
        VLOG(4) << "Trying to auto-merge...";
        if (!tryAutoMerge(original_view, conflict_view, &item)) {
          return true;
        }
      } else {
        CHECK(mode == ConflictTraversalMode::kPrepareManualMerge);
        conflicts->push_back({conflict_view.get(item.first), item.second});
      }
    } else {
      if (mode == ConflictTraversalMode::kPrepareManualMerge) {
        conflict_free_part->updates_.emplace(item);
      }
    }
  }

  for (const RemoveMap::value_type& item : removes_) {
    if (potential_conflicts.count(item.first) != 0u) {
      if (mode == ConflictTraversalMode::kTryMergeOrBail) {
        VLOG(4) << "Remove conflict!";
        return true;
      } else {
        CHECK(mode == ConflictTraversalMode::kPrepareManualMerge);
        conflicts->push_back({conflict_view.get(item.first), item.second});
      }
    } else {
      if (mode == ConflictTraversalMode::kPrepareManualMerge) {
        conflict_free_part->removes_.emplace(item);
      }
    }
  }

  return false;
}

bool DeltaView::tryAutoMerge(const ViewBase& original_view,
                             const ViewBase& conflict_view,
                             UpdateMap::value_type* item) const {
  CHECK_NOTNULL(item);
  std::shared_ptr<const Revision> conflicting_revision =
      conflict_view.get(item->first);
  std::shared_ptr<const Revision> original_revision =
      original_view.get(item->first);
  CHECK(conflicting_revision);
  // Original revision must exist, since db_stamp > begin_time_ and the
  // transaction wouldn't know about the item unless it existed before
  // begin_time_.
  CHECK(original_revision);
  return item->second->tryAutoMerge(*conflicting_revision, *original_revision,
                                    table_.getAutoMergePolicies());
}

}  // namespace internal
}  // namespace map_api
