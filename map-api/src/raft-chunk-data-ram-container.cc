#include "map-api/raft-chunk-data-ram-container.h"

namespace map_api {

RaftChunkDataRamContainer::~RaftChunkDataRamContainer() {}

bool RaftChunkDataRamContainer::initImpl() { return true; }

std::shared_ptr<const Revision> RaftChunkDataRamContainer::getByIdImpl(
    const common::Id& id, const LogicalTime& time) const {
  HistoryMap::const_iterator found = data_.find(id);
  if (found == data_.end()) {
    return std::shared_ptr<Revision>();
  }
  History::const_iterator latest = found->second.latestAt(time);
  if (latest == found->second.end()) {
    return Revision::ConstPtr();
  }
  return *latest;
}

void RaftChunkDataRamContainer::findByRevisionImpl(
    int key, const Revision& value_holder, const LogicalTime& time,
    ConstRevisionMap* dest) const {
  CHECK_NOTNULL(dest)->clear();
  forEachItemFoundAtTime(key, value_holder, time,
                         [&dest](const common::Id& id,
                                 const Revision::ConstPtr& item) {
    CHECK(dest->emplace(id, item).second);
  });
}

void RaftChunkDataRamContainer::getAvailableIdsImpl(
    const LogicalTime& time, std::vector<common::Id>* ids) const {
  CHECK_NOTNULL(ids)->clear();
  ids->reserve(data_.size());
  for (const HistoryMap::value_type& pair : data_) {
    History::const_iterator latest = pair.second.latestAt(time);
    if (latest != pair.second.cend()) {
      if (!(*latest)->isRemoved()) {
        ids->emplace_back(pair.first);
      }
    }
  }
}

int RaftChunkDataRamContainer::countByRevisionImpl(
    int key, const Revision& value_holder, const LogicalTime& time) const {
  int count = 0;
  forEachItemFoundAtTime(key, value_holder, time,
                         [&count](const common::Id& /*id*/,
                                  const Revision::ConstPtr& /*item*/) {
    ++count;
  });
  return count;
}

void RaftChunkDataRamContainer::chunkHistory(const common::Id& chunk_id,
                                             const LogicalTime& time,
                                             HistoryMap* dest) const {
  // TODO(aqurai): Safe to lock access_mutex_ here?
  CHECK_NOTNULL(dest)->clear();
  for (const HistoryMap::value_type& pair : data_) {
    if ((*pair.second.begin())->getChunkId() == chunk_id) {
      CHECK(dest->emplace(pair).second);
    }
  }
  trimToTime(time, dest);
}

bool RaftChunkDataRamContainer::checkAndPrepareInsert(
    const LogicalTime& time, const std::shared_ptr<Revision>& query) {
  CHECK(query.get() != nullptr);
  CHECK(isInitialized()) << "Attempted to insert into non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(query->structureMatch(*reference))
      << "Bad structure of insert revision";
  CHECK(query->getId<common::Id>().isValid())
      << "Attempted to insert element with invalid ID";
  query->setInsertTime(time);
  query->setUpdateTime(time);
  return true;
}

bool RaftChunkDataRamContainer::checkAndPrepareUpdate(
    const LogicalTime& time, const std::shared_ptr<Revision>& query) {
  CHECK(query != nullptr);
  CHECK(isInitialized()) << "Attempted to update in non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(query->structureMatch(*reference))
      << "Bad structure of update revision";
  CHECK(query->getId<common::Id>().isValid())
      << "Attempted to update element with invalid ID";
  LogicalTime update_time = time;
  query->setUpdateTime(update_time);
  return true;
}

bool RaftChunkDataRamContainer::checkAndPrepareBulkInsert(
    const LogicalTime& time, const MutableRevisionMap& query) {
  CHECK(isInitialized()) << "Attempted to insert into non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  common::Id id;
  for (const typename MutableRevisionMap::value_type& id_revision : query) {
    CHECK_NOTNULL(id_revision.second.get());
    CHECK(id_revision.second->structureMatch(*reference))
        << "Bad structure of insert revision";
    id = id_revision.second->getId<common::Id>();
    CHECK(id.isValid()) << "Attempted to insert element with invalid ID";
    CHECK(id == id_revision.first) << "ID in RevisionMap doesn't match";
    id_revision.second->setInsertTime(time);
    id_revision.second->setUpdateTime(time);
  }
  return true;
}

bool RaftChunkDataRamContainer::checkAndPrepareRemove(
    const LogicalTime& time, const std::shared_ptr<Revision>& query) {
  CHECK(query.get() != nullptr);
  CHECK(isInitialized()) << "Attempted to insert into non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(query->structureMatch(*reference))
      << "Bad structure of insert revision";
  CHECK(query->getId<common::Id>().isValid())
      << "Attempted to insert element with invalid ID";
  query->setUpdateTime(time);
  query->setRemoved();
  return true;
}

bool RaftChunkDataRamContainer::checkAndPatch(
    const std::shared_ptr<Revision>& query) {
  std::lock_guard<std::mutex> lock(access_mutex_);
  CHECK(query != nullptr);
  CHECK(isInitialized()) << "Attempted to insert into non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(query->structureMatch(*reference)) << "Bad structure of patch revision";
  CHECK(query->getId<common::Id>().isValid())
      << "Attempted to insert element with invalid ID";
  // TODO(aqurai): Remove this.
  // LOG(WARNING) << PeerId::self() << ": Patching in table " << name()
  //            << ", History size = " << data_.size();
  return patch(query);
}

bool RaftChunkDataRamContainer::patch(const Revision::ConstPtr& query) {
  CHECK(query != nullptr);
  common::Id id = query->getId<common::Id>();
  LogicalTime time = query->getUpdateTime();
  HistoryMap::iterator found = data_.find(id);
  if (found == data_.end()) {
    found = data_.insert(std::make_pair(id, History())).first;
  }
  for (History::iterator it = found->second.begin(); it != found->second.end();
       ++it) {
    if ((*it)->getUpdateTime() <= time) {
      CHECK_NE(time, (*it)->getUpdateTime());
      found->second.insert(it, query);
      return true;
    }
    LOG(WARNING) << "Patching, not in front!";  // shouldn't usually be the case
  }
  found->second.push_back(query);
  return true;
}

RaftChunkDataRamContainer::RaftLog::RaftLog() : commit_index_(0) {}

RaftChunkDataRamContainer::RaftLog::iterator
RaftChunkDataRamContainer::RaftLog::getLogIteratorByIndex(uint64_t index) {
  iterator it = end();
  if (empty()) {
    return end();
  }
  if (index < front()->index() || index > back()->index()) {
    return end();
  } else {
    iterator it;
    // The log indices are always sequential.
    it = begin() + (index - front()->index());
    CHECK_EQ((*it)->index(), index) << " Log entries size = " << size();
    return it;
  }
}

RaftChunkDataRamContainer::RaftLog::const_iterator
RaftChunkDataRamContainer::RaftLog::getConstLogIteratorByIndex(uint64_t index) const {
  const_iterator it = cend();
  if (empty()) {
    return cend();
  }
  if (index < front()->index() || index > back()->index()) {
    return cend();
  } else {
    const_iterator it;
    // The log indices are always sequential.
    it = cbegin() + (index - front()->index());
    CHECK_EQ((*it)->index(), index) << " Log entries size = " << size();
    return it;
  }
}

uint64_t RaftChunkDataRamContainer::RaftLog::eraseAfter(iterator it) {
  CHECK((it + 1) != begin());
  resize(std::distance(begin(), it + 1));
  return lastLogIndex();
}

uint64_t RaftChunkDataRamContainer::RaftLog::setEntryCommitted(iterator it) {
  CHECK_EQ(commit_index_ + 1, (*it)->index());
  return ++commit_index_;
}

RaftChunkDataRamContainer::LogReadAccess::LogReadAccess(
    const RaftChunkDataRamContainer* container)
    : read_log_(&container->log_),
      is_enabled_(true) {
  read_log_->mutex()->acquireReadLock();
}

const RaftChunkDataRamContainer::RaftLog*
RaftChunkDataRamContainer::LogReadAccess::operator->() const {
  if (is_enabled_) {
    return read_log_;
  } else {
    LOG(FATAL) << "Tried to access raft log using a disabled LogReadAccess object";
    return NULL;
  }
}

void RaftChunkDataRamContainer::LogReadAccess::unlockAndDisable() {
  if (is_enabled_) {
    is_enabled_ = false;
    read_log_->mutex()->releaseReadLock();
  } else {
    LOG(FATAL) << "Tried to unlock/disable an already disabled LogReadAccess object";
  }
}

RaftChunkDataRamContainer::LogReadAccess::~LogReadAccess() {
  if (is_enabled_) {
    read_log_->mutex()->releaseReadLock();
  }
}

RaftChunkDataRamContainer::LogWriteAccess::LogWriteAccess(
    RaftChunkDataRamContainer* container)
    : write_log_(&container->log_),
      is_enabled_(true) {
  write_log_->mutex()->acquireWriteLock();
}

RaftChunkDataRamContainer::RaftLog* RaftChunkDataRamContainer::LogWriteAccess::
operator->() const {
  if (is_enabled_) {
    return write_log_;
  } else {
    LOG(FATAL) << "Tried to access raft log using a disabled LogWriteAccess object";
    return NULL;
  }
}

void RaftChunkDataRamContainer::LogWriteAccess::unlockAndDisable() {
  if (is_enabled_) {
    is_enabled_ = false;
    write_log_->mutex()->releaseWriteLock();
  } else {
    LOG(FATAL) << "Tried to unlock/disable an already disabled LogWriteAccess object";
  }
}

RaftChunkDataRamContainer::LogWriteAccess::~LogWriteAccess() {
  if (is_enabled_) {
    write_log_->mutex()->releaseWriteLock();
  }
}

}  // namespace map_api
