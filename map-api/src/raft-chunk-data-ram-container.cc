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

proto::RaftLogEntry* RaftChunkDataRamContainer::RaftLog::copyWithoutRevision(
    const const_iterator& it) const {
  proto::RaftLogEntry* entry = new proto::RaftLogEntry;

  entry->set_index((*it)->index());
  entry->set_term((*it)->term());
  if ((*it)->has_sender()) {
    entry->set_sender((*it)->sender());
  }
  if ((*it)->has_sender_serial_id()) {
    entry->set_sender_serial_id((*it)->sender_serial_id());
  }
  if ((*it)->has_add_peer()) {
    entry->set_add_peer((*it)->add_peer());
    entry->set_is_rejoin_peer((*it)->is_rejoin_peer());
  }
  if ((*it)->has_remove_peer()) {
    entry->set_remove_peer((*it)->remove_peer());
  }
  if ((*it)->has_lock_peer()) {
    entry->set_lock_peer((*it)->lock_peer());
  }
  if ((*it)->has_unlock_peer()) {
    entry->set_unlock_peer((*it)->unlock_peer());
  }
  if ((*it)->has_unlock_proceed_commits()) {
    entry->set_unlock_proceed_commits((*it)->unlock_proceed_commits());
  }
  if ((*it)->has_unlock_lock_index()) {
    entry->set_unlock_lock_index((*it)->unlock_lock_index());
  }
  if ((*it)->has_multi_chunk_transaction_info()) {
    entry->mutable_multi_chunk_transaction_info()->CopyFrom(
        (*it)->multi_chunk_transaction_info());
  }
  if ((*it)->has_multi_chunk_transaction_num_entries()) {
    entry->set_multi_chunk_transaction_num_entries(
        (*it)->multi_chunk_transaction_num_entries());
  }
  if ((*it)->has_revision_id()) {
    entry->mutable_revision_id()->CopyFrom((*it)->revision_id());
  }
  if ((*it)->has_logical_time()) {
    entry->set_logical_time((*it)->logical_time());
  }
  return entry;
}

uint64_t RaftChunkDataRamContainer::RaftLog::getEntryIndex(
    const PeerId& peer, uint64_t serial_id) const {
  for (const_reverse_iterator it = rbegin(); it != rend(); ++it) {
    if ((*it)->has_sender() && peer.ipPort().compare((*it)->sender()) == 0) {
      CHECK((*it)->has_sender_serial_id());
      if ((*it)->sender_serial_id() == serial_id) {
        return (*it)->index();
      }
    }
  }
  return 0;
}

uint64_t RaftChunkDataRamContainer::RaftLog::getPeerLatestSerialId(
    const PeerId& peer) const {
  if (serial_id_map_.count(peer.ipPort()) == 1) {
    return serial_id_map_.find(peer.ipPort())->second;
  }
  return 0;
}

uint64_t RaftChunkDataRamContainer::RaftLog::eraseAfter(const iterator& it) {
  CHECK((it + 1) != begin());
  resize(std::distance(begin(), it + 1));
  return lastLogIndex();
}

void RaftChunkDataRamContainer::RaftLog::appendLogEntry(
    const std::shared_ptr<proto::RaftLogEntry>& entry) {
  push_back(entry);
  if (entry->has_sender()) {
    CHECK(entry->has_sender_serial_id());
    serial_id_map_[entry->sender()] = entry->sender_serial_id();
  }
}

uint64_t RaftChunkDataRamContainer::RaftLog::setEntryCommitted(
    const iterator& it) {
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
