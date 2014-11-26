#include <map-api/chunk-transaction.h>
#include <unordered_set>

#include <map-api/cru-table.h>
#include <map-api/net-table.h>

namespace map_api {

ChunkTransaction::ChunkTransaction(Chunk* chunk)
    : ChunkTransaction(LogicalTime::sample(), chunk) {}

ChunkTransaction::ChunkTransaction(const LogicalTime& begin_time, Chunk* chunk)
    : begin_time_(begin_time), chunk_(CHECK_NOTNULL(chunk)) {
  CHECK(begin_time < LogicalTime::sample());
  insertions_.clear();
  updates_.clear();
  structure_reference_ = chunk_->underlying_table_->getTemplate();
}

CRTable::RevisionMap ChunkTransaction::dumpChunk() {
  CRTable::RevisionMap result;
  chunk_->dumpItems(begin_time_, &result);
  return result;
}

void ChunkTransaction::insert(std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(revision.get());
  CHECK(revision->structureMatch(*structure_reference_));
  Id id = revision->getId<Id>();
  CHECK(id.isValid());
  CHECK(insertions_.emplace(id, revision).second);
}

void ChunkTransaction::update(std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(revision.get());
  CHECK(revision->structureMatch(*structure_reference_));
  CHECK(chunk_->underlying_table_->type() == CRTable::Type::CRU);
  Id id = revision->getId<Id>();
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
  CHECK(chunk_->underlying_table_->type() == CRTable::Type::CRU);
  Id id = revision->getId<Id>();
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
  CHECK(chunk_->isLocked());
  std::unordered_map<Id, LogicalTime> stamps;
  prepareCheck(LogicalTime::sample(), &stamps);
  // The following check may be left out if too costly
  for (const std::pair<const Id, std::shared_ptr<const Revision> >& item :
       insertions_) {
    if (stamps.find(item.first) != stamps.end()) {
      LOG(ERROR) << "Table " << chunk_->underlying_table_->name()
                 << " already contains id " << item.first;
      return false;
    }
  }
  for (const std::pair<const Id, std::shared_ptr<const Revision> >& item :
       updates_) {
    if (stamps[item.first] >= begin_time_) {
      return false;
    }
  }
  for (const std::pair<const Id, std::shared_ptr<const Revision> >& item :
       removes_) {
    if (stamps[item.first] >= begin_time_) {
      return false;
    }
  }
  for (const ChunkTransaction::ConflictCondition& item : conflict_conditions_) {
    CRTable::RevisionMap dummy;
    chunk_->underlying_table_->findByRevision(item.key, *item.value_holder,
                                              LogicalTime::sample(), &dummy);
    if (!dummy.empty()) {
      return false;
    }
  }
  return true;
}

void ChunkTransaction::checkedCommit(const LogicalTime& time) {
  InsertMap::iterator iter;
  for (iter = insertions_.begin(); iter != insertions_.end();) {
    if (removes_.count(iter->first) > 0u) {
      iter = insertions_.erase(iter);
    } else {
      ++iter;
    }
  }
  chunk_->bulkInsertLocked(insertions_, time);
  for (const std::pair<const Id, std::shared_ptr<Revision> >& item : updates_) {
    if (removes_.count(item.first) == 0u) {
      chunk_->updateLocked(time, item.second);
    }
  }
  for (const std::pair<const Id, std::shared_ptr<Revision> >& item : removes_) {
    chunk_->removeLocked(time, item.second);
  }
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
  std::unordered_map<Id, LogicalTime> stamps;
  prepareCheck(merge_transaction->begin_time_, &stamps);
  // The following check may be left out if too costly
  for (const typename CRTable::NonConstRevisionMap::value_type& item :
       insertions_) {
    CHECK(stamps.find(item.first) == stamps.end()) << "Insert conflict!";
    merge_transaction->insertions_.insert(item);
  }
  for (const typename CRTable::NonConstRevisionMap::value_type& item :
       updates_) {
    if (stamps[item.first] >= begin_time_) {
      conflicts->push_back(
          {merge_transaction->getById(item.first), item.second});
    } else {
      merge_transaction->updates_.insert(item);
    }
  }
  for (const typename CRTable::NonConstRevisionMap::value_type& item :
       removes_) {
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
    std::unordered_map<Id, LogicalTime>* chunk_stamp) {
  CHECK_NOTNULL(chunk_stamp);
  chunk_stamp->clear();
  CRTable::RevisionMap contents;
  // same as "chunk_->dumpItems(LogicalTime::sample(), &contents);" without the
  // locking (because that is already done)
  chunk_->underlying_table_->dumpChunk(chunk_->id(), check_time, &contents);
  LogicalTime time;
  if (!updates_.empty()) {
    CHECK(chunk_->underlying_table_->type() == CRTable::Type::CRU);
    for (const CRTable::RevisionMap::value_type& item : contents) {
      time = item.second->getUpdateTime();
      chunk_stamp->insert(std::make_pair(item.first, time));
    }
  } else {
    for (const CRTable::RevisionMap::value_type& item : contents) {
      chunk_stamp->insert(std::make_pair(item.first, time));
    }
  }
}

} /* namespace map_api */
