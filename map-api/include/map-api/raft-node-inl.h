#ifndef MAP_API_RAFT_NODE_INL_H_
#define MAP_API_RAFT_NODE_INL_H_

#include <string>

namespace map_api {

void RaftNode::handleQueryReadyToCommit(
    const proto::MultiChunkTransactionQuery& query, const PeerId& sender,
    Message* response) {
  multi_chunk_transaction_manager_->handleQueryReadyToCommit(query, sender,
                                                             response);
}

void RaftNode::handleCommitNotification(
    const proto::MultiChunkTransactionQuery& query, const PeerId& sender,
    Message* response) {
  multi_chunk_transaction_manager_->handleCommitNotification(query, sender,
                                                             response);
}

void RaftNode::handleAbortNotification(
    const proto::MultiChunkTransactionQuery& query, const PeerId& sender,
    Message* response) {
  multi_chunk_transaction_manager_->handleAbortNotification(query, sender,
                                                            response);
}

bool RaftNode::isCommitIndexInCurrentTerm() const {
  uint64_t current_term = getTerm();
  LogReadAccess log_reader(data_);
  ConstLogIterator it =
      log_reader->getConstLogIteratorByIndex(log_reader->commitIndex());
  CHECK(it != log_reader->cend());
  if ((*it)->term() >= current_term) {
    return true;
  }
  return false;
}

void RaftNode::updateHeartbeatTime() const {
  std::lock_guard<std::mutex> heartbeat_lock(last_heartbeat_mutex_);
  last_heartbeat_ = std::chrono::system_clock::now();
}

double RaftNode::getTimeSinceHeartbeatMs() {
  TimePoint last_hb_time;
  {
    std::lock_guard<std::mutex> lock(last_heartbeat_mutex_);
    last_hb_time = last_heartbeat_;
  }
  TimePoint now = std::chrono::system_clock::now();
  return static_cast<double>(
      std::chrono::duration_cast<std::chrono::milliseconds>(now - last_hb_time)
          .count());
}

bool RaftNode::hasPeer(const PeerId& peer) {
  std::lock_guard<std::mutex> peer_lock(peer_mutex_);
  return peer_list_.count(peer);
}

size_t RaftNode::numPeers() {
  std::lock_guard<std::mutex> peer_lock(peer_mutex_);
  return peer_list_.size();
}

void RaftNode::setAppendEntriesResponse(proto::AppendResponseStatus status,
                                        uint64_t current_commit_index,
                                        uint64_t current_term,
                                        uint64_t last_log_index,
                                        uint64_t last_log_term,
                                        proto::AppendEntriesResponse* response)
                                            const {
  CHECK_NOTNULL(response);
  response->set_response(status);
  response->set_commit_index(current_commit_index);
  response->set_term(current_term);
  response->set_last_log_index(last_log_index);
  response->set_last_log_term(last_log_term);
}

const std::string RaftNode::getLogEntryTypeString(
    const std::shared_ptr<proto::RaftLogEntry>& entry) const {
  if (entry->has_add_peer()) {
    return kRaftLogEntryAddPeer;
  } else if (entry->has_remove_peer()) {
    return kRaftLogEntryRemovePeer;
  } else if (entry->has_lock_peer()) {
    return kRaftLogEntryLockRequest;
  } else if (entry->has_unlock_peer()) {
    return kRaftLogEntryUnlockRequest;
  } else if (entry->has_insert_revision() || entry->has_revision_id()) {
    return kRaftLogEntryInsertRevision;
  } else if (entry->bulk_insert_revision_size() > 0 ||
             entry->bulk_inserted_revision_id_list_size() > 0) {
    return kRaftLogEntryBulkInsertRevision;
  } else if (entry->has_multi_chunk_transaction_info()) {
    return kRaftLogEntryRaftTransactionInfo;
  } else {
    return kRaftLogEntryOther;
  }
}

template <typename RequestType>
void RaftNode::fillMetadata(RequestType* destination) const {
  CHECK_NOTNULL(destination);
  destination->mutable_metadata()->set_table(this->table_name_);
  this->chunk_id_.serialize(
      destination->mutable_metadata()->mutable_chunk_id());
}

void RaftNode::grantChunkLockFromQueue(const LogWriteAccess& log_writer,
                                       const uint64_t current_term) {
  if (!raft_chunk_lock_.isLocked() && !lock_queue_.empty()) {
    while (!lock_queue_.empty()) {
      const PeerId lock_contender = lock_queue_.front();
      if (hasPeer(lock_contender) || PeerId::self() == lock_contender) {
        std::shared_ptr<proto::RaftLogEntry> entry(new proto::RaftLogEntry);
        entry->set_queued_lock_peer(lock_contender.ipPort());
        leaderAppendLogEntryLocked(log_writer, entry, current_term);
        VLOG(3) << PeerId::self() << chunk_id_
                << ": Granting lock from queue to " << lock_contender
                << " with entry index " << entry->index();
        return;
      }
    }
  }
}

}  // namespace map_api

#endif  // MAP_API_RAFT_NODE_INL_H_
