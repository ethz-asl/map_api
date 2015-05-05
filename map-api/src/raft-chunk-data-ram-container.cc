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
  CHECK_NOTNULL(dest);
  dest->clear();
  forEachItemFoundAtTime(key, value_holder, time,
                         [&dest](const common::Id& id,
                                 const Revision::ConstPtr& item) {
    CHECK(dest->find(id) == dest->end());
    CHECK(dest->emplace(id, item).second);
  });
}

void RaftChunkDataRamContainer::getAvailableIdsImpl(
    const LogicalTime& time, std::vector<common::Id>* ids) const {
  CHECK_NOTNULL(ids);
  ids->clear();
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

RaftChunkDataRamContainer::RaftLog::iterator
RaftChunkDataRamContainer::RaftLog::getLogIteratorByIndex(uint64_t index) {
  iterator it = end();
  if (index < front()->index() || index > back()->index()) {
    return it;
  } else {
    // The log indices are always sequential.
    it = begin() + (index - front()->index());
    CHECK_EQ((*it)->index(), index) << " Log entries size = " << size();
    return it;
  }
}

RaftChunkDataRamContainer::RaftLog::const_iterator
RaftChunkDataRamContainer::RaftLog::getConstLogIteratorByIndex(uint64_t index) const {
  const_iterator it = cend();
  if (index < front()->index() || index > back()->index()) {
    return it;
  } else {
    // The log indices are always sequential.
    it = cbegin() + (index - front()->index());
    CHECK_EQ((*it)->index(), index) << " Log entries size = " << size();
    return it;
  }
}

uint64_t RaftChunkDataRamContainer::RaftLog::eraseAfter(iterator it) {
  CHECK(it + 1 != begin());
  resize(std::distance(begin(), it + 1));
  return lastLogIndex();
}

RaftChunkDataRamContainer::LogReadAccess::LogReadAccess(
    const RaftChunkDataRamContainer* container)
    : read_log_(&container->log_),
      is_enabled_(true) {
  read_log_->mutex()->acquireReadLock();
}

const RaftChunkDataRamContainer::RaftLog*
RaftChunkDataRamContainer::LogReadAccess::
operator->() const {
  if (is_enabled_) {
    return read_log_;
  } else {
    LOG(FATAL) << "Tried to access raft log using a disabled LogReadAccess object";
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
