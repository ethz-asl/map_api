#include "map-api/chunk-transaction.h"

#include <unordered_set>

#include "map-api/cru-table.h"

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

std::shared_ptr<Revision> ChunkTransaction::getById(const Id& id) {
  std::shared_ptr<Revision> result = getByIdFromUncommitted(id);
  if (result != nullptr) {
    return result;
  }
  chunk_->readLock();
  result = chunk_->underlying_table_->getById(id, begin_time_);
  chunk_->unlock();
  return result;
}

std::shared_ptr<Revision> ChunkTransaction::getByIdFromUncommitted(const Id& id)
    const {
  UpdateMap::const_iterator updated = updates_.find(id);
  if (updated != updates_.end()) {
    return updated->second;
  }
  InsertMap::const_iterator inserted = insertions_.find(id);
  if (inserted != insertions_.end()) {
    return inserted->second;
  }
  return std::shared_ptr<Revision>();
}

CRTable::RevisionMap ChunkTransaction::dumpChunk() {
  CRTable::RevisionMap result;
  chunk_->dumpItems(begin_time_, &result);
  return result;
}

void ChunkTransaction::insert(std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(revision.get());
  CHECK(revision->structureMatch(*structure_reference_));
  Id id;
  revision->get(CRTable::kIdField, &id);
  CHECK(insertions_.insert(std::make_pair(id, revision)).second);
}

void ChunkTransaction::update(std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(revision.get());
  CHECK(revision->structureMatch(*structure_reference_));
  CHECK(chunk_->underlying_table_->type() == CRTable::Type::CRU);
  Id id;
  revision->get(CRTable::kIdField, &id);
  CHECK(updates_.insert(std::make_pair(id, revision)).second);
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
  CRTable::RevisionMap contents;
  // TODO(tcies) caching entire table is not a long-term solution
  chunk_->underlying_table_->dump(LogicalTime::sample(), &contents);
  std::unordered_set<Id> present_ids;
  for (const CRTable::RevisionMap::value_type& item : contents) {
    present_ids.insert(item.first);
  }
  // The following check may be left out if too costly
  for (const std::pair<const Id, std::shared_ptr<Revision> >& item :
       insertions_) {
    if (present_ids.find(item.first) != present_ids.end()) {
      LOG(ERROR) << "Table " << chunk_->underlying_table_->name()
                 << " already contains id " << item.first;
      return false;
    }
  }
  std::unordered_map<Id, LogicalTime> update_times;
  if (!updates_.empty()) {
    CHECK(chunk_->underlying_table_->type() == CRTable::Type::CRU);
    // TODO(tcies) caching entire table is not a long-term solution (but maybe
    // caching the entire chunk could be?)
    for (const CRTable::RevisionMap::value_type& item : contents) {
      LogicalTime update_time;
      item.second->get(CRUTable::kUpdateTimeField, &update_time);
      update_times[item.first] = update_time;
    }
  }
  for (const std::pair<const Id, std::shared_ptr<Revision> >& item : updates_) {
    if (update_times[item.first] >= begin_time_) {
      return false;
    }
  }
  for (const ChunkTransaction::ConflictCondition& item : conflict_conditions_) {
    CRTable::RevisionMap dummy;
    if (chunk_->underlying_table_->findByRevision(
            item.key, *item.value_holder, LogicalTime::sample(), &dummy) > 0) {
      return false;
    }
  }
  return true;
}

void ChunkTransaction::checkedCommit(const LogicalTime& time) {
  chunk_->bulkInsertLocked(insertions_, time);
  for (const std::pair<const Id, std::shared_ptr<Revision> >& item : updates_) {
    chunk_->updateLocked(time, item.second.get());
  }
}

} /* namespace map_api */
