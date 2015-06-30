#include "map-api/chunk-transaction.h"

#include <unordered_set>

#include <multiagent-mapping-common/accessors.h>

#include "map-api/net-table.h"

namespace map_api {

ChunkTransaction::ChunkTransaction(ChunkBase* chunk, NetTable* table)
    : ChunkTransaction(LogicalTime::sample(), chunk, table) {}

ChunkTransaction::ChunkTransaction(const LogicalTime& begin_time,
                                   ChunkBase* chunk, NetTable* table)
    : begin_time_(begin_time),
      chunk_(CHECK_NOTNULL(chunk)),
      table_(CHECK_NOTNULL(table)) {
  CHECK(begin_time < LogicalTime::sample());
  insertions_.clear();
  updates_.clear();
  structure_reference_ = chunk_->data_container_->getTemplate();
}

void ChunkTransaction::dumpChunk(ConstRevisionMap* result) {
  CHECK_NOTNULL(result);
  chunk_->dumpItems(begin_time_, result);

  // Add previously committed items.
  for (ItemTimes::const_iterator it = previously_committed_.begin();
       it != previously_committed_.end(); ++it) {
    std::shared_ptr<const Revision> item =
        chunk_->data_container_->getById(it->first, it->second);
    if (item) {
      (*result)[it->first] = item;
    } else {  // Item has been deleted in a previous commit.
      ConstRevisionMap::iterator found = result->find(it->first);
      if (found != result->end()) {
        result->erase(found);
      }
    }
  }
}

void ChunkTransaction::insert(std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(revision.get());
  CHECK(revision->structureMatch(*structure_reference_));
  common::Id id = revision->getId<common::Id>();
  CHECK(id.isValid());
  CHECK(insertions_.emplace(id, revision).second);
}

void ChunkTransaction::update(std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(revision.get());
  CHECK(revision->structureMatch(*structure_reference_));
  common::Id id = revision->getId<common::Id>();
  CHECK(id.isValid());
  InsertMap::iterator uncommitted = insertions_.find(id);
  if (uncommitted != insertions_.end()) {
    // If this updates a revision added also in this transaction, the insertion
    // is replaced with the update, in order to ensure the setting of default
    // fields such as insert time and chunk id.
    uncommitted->second = revision;
  } else {
    // Assignment, as later updates supersede earlier ones.
    updates_[id] = revision;
  }
}

void ChunkTransaction::remove(std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(revision.get());
  CHECK(revision->structureMatch(*structure_reference_));
  common::Id id = revision->getId<common::Id>();
  CHECK(id.isValid());
  CHECK(removes_.emplace(id, revision).second);
  // TODO(tcies) situation uncommitted
}

bool ChunkTransaction::commit() {
  chunk_->writeLock();
  if (!check()) {
    chunk_->unlock();
    return false;
  }
  checkedCommit(LogicalTime::sample());
  chunk_->unlock();
  return true;
}

bool ChunkTransaction::check() {
  CHECK(chunk_->isWriteLocked());
  std::unordered_map<common::Id, LogicalTime> stamps;
  prepareCheck(LogicalTime::sample(), &stamps);
  // The following check may be left out if too costly
  for (const std::pair<const common::Id,
      std::shared_ptr<const Revision> >& item : insertions_) {
    if (stamps.find(item.first) != stamps.end()) {
      LOG(ERROR) << "Table " << chunk_->data_container_->name()
                 << " already contains id " << item.first;
      return false;
    }
  }
  for (const std::pair<const common::Id,
      std::shared_ptr<const Revision> >& item : updates_) {
    if (hasUpdateConflict(item.first, stamps)) {
      return false;
    }
  }
  for (const std::pair<const common::Id,
      std::shared_ptr<const Revision> >& item : removes_) {
    if (hasUpdateConflict(item.first, stamps)) {
      return false;
    }
  }
  for (const ChunkTransaction::ConflictCondition& item : conflict_conditions_) {
    ConstRevisionMap dummy;
    chunk_->data_container_->findByRevision(item.key, *item.value_holder,
                                            LogicalTime::sample(), &dummy);
    if (!dummy.empty()) {
      return false;
    }
  }
  return true;
}

void ChunkTransaction::checkedCommit(const LogicalTime& time) {
  for (InsertMap::iterator iter = insertions_.begin();
       iter != insertions_.end();) {
    if (removes_.count(iter->first) > 0u) {
      iter = insertions_.erase(iter);
    } else {
      previously_committed_[iter->first] = time;
      ++iter;
    }
  }
  chunk_->bulkInsertLocked(insertions_, time);

  for (const std::pair<const common::Id,
      std::shared_ptr<Revision> >& item : updates_) {
    if (removes_.count(item.first) == 0u) {
      chunk_->updateLocked(time, item.second);
      previously_committed_[item.first] = time;
    }
  }
  for (const std::pair<const common::Id,
      std::shared_ptr<Revision> >& item : removes_) {
    chunk_->removeLocked(time, item.second);
    previously_committed_[item.first] = time;
  }

  insertions_.clear();
  updates_.clear();
  removes_.clear();
}

void ChunkTransaction::merge(
    const std::shared_ptr<ChunkTransaction>& merge_transaction,
    Conflicts* conflicts) {
  CHECK_NOTNULL(merge_transaction.get());
  CHECK_NOTNULL(conflicts);
  CHECK(conflict_conditions_.empty()) << "merge not compatible with conflict "
                                         "conditions";
  conflicts->clear();
  chunk_->readLock();
  std::unordered_map<common::Id, LogicalTime> stamps;
  prepareCheck(merge_transaction->begin_time_, &stamps);
  // The following check may be left out if too costly
  for (const typename MutableRevisionMap::value_type& item : insertions_) {
    CHECK(stamps.find(item.first) == stamps.end()) << "Insert conflict!";
    merge_transaction->insertions_.insert(item);
  }
  for (const typename MutableRevisionMap::value_type& item : updates_) {
    if (stamps[item.first] >= begin_time_) {
      conflicts->push_back(
          {merge_transaction->getById(item.first), item.second});
    } else {
      merge_transaction->updates_.insert(item);
    }
  }
  for (const typename MutableRevisionMap::value_type& item : removes_) {
    if (stamps[item.first] >= begin_time_) {
      conflicts->push_back(
          {merge_transaction->getById(item.first), item.second});
    } else {
      merge_transaction->removes_.insert(item);
    }
  }
  chunk_->unlock();
}

size_t ChunkTransaction::numChangedItems() const {
  CHECK(conflict_conditions_.empty()) << "changeCount not compatible with "
                                         "conflict conditions";
  return insertions_.size() + updates_.size() + removes_.size();
}

void ChunkTransaction::prepareCheck(
    const LogicalTime& check_time,
    std::unordered_map<common::Id, LogicalTime>* chunk_stamp) const {
  CHECK_NOTNULL(chunk_stamp);
  chunk_stamp->clear();
  ConstRevisionMap contents;
  // same as "chunk_->dumpItems(LogicalTime::sample(), &contents);" without the
  // locking (because that is already done)
  chunk_->data_container_->dump(check_time, &contents);
  LogicalTime time;
  if (!updates_.empty()) {
    for (const ConstRevisionMap::value_type& item : contents) {
      time = item.second->getUpdateTime();
      chunk_stamp->insert(std::make_pair(item.first, time));
    }
  } else {
    for (const ConstRevisionMap::value_type& item : contents) {
      chunk_stamp->insert(std::make_pair(item.first, time));
    }
  }
}

bool ChunkTransaction::hasUpdateConflict(const common::Id& item,
                                         const ItemTimes& db_stamps) const {
  const LogicalTime db_stamp = getChecked(db_stamps, item);
  if (db_stamp >= begin_time_) {
    // Allow conflicts only if they come from a previous commit of the same
    // transaction.
    ItemTimes::const_iterator found = previously_committed_.find(item);
    if (found != previously_committed_.end()) {
      if (found->second == db_stamp) {
        return false;
      }
    }
    return true;
  }
  return false;
}

void ChunkTransaction::getTrackers(
    const NetTable::NewChunkTrackerMap& overrides,
    TableToIdMultiMap* trackers) const {
  CHECK_NOTNULL(trackers);
  for (const typename NetTable::NewChunkTrackerMap::value_type&
           table_tracker_getter : table_->new_chunk_trackers()) {
    NetTable::NewChunkTrackerMap::const_iterator override_it =
        overrides.find(table_tracker_getter.first);
    const std::function<common::Id(const Revision&)>& tracker_id_extractor =
        ((override_it != overrides.end()) ? (override_it->second)
                                          : (table_tracker_getter.second));
    for (const InsertMap::value_type& insertion : insertions_) {
      common::Id id = tracker_id_extractor(*insertion.second);
      trackers->emplace(table_tracker_getter.first, id);
    }
  }
}

}  // namespace map_api
