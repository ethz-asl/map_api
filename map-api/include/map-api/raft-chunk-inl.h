#ifndef MAP_API_RAFT_CHUNK_INL_H_
#define MAP_API_RAFT_CHUNK_INL_H_

#include <mutex>

#include "map-api/message.h"

namespace map_api {

void RaftChunk::setStateFollowerAndStartRaft() {
    raft_node_.state_ = RaftNode::State::FOLLOWER;
    VLOG(2) << PeerId::self() << ": Starting Raft node as follower for chunk "
            << id_.printString();
    raft_node_.start();
}

void RaftChunk::setStateLeaderAndStartRaft() {
    raft_node_.state_ = RaftNode::State::LEADER;
    VLOG(2) << PeerId::self() << ": Starting Raft node as leader for chunk "
            << id_.printString();
    raft_node_.start();
}

int RaftChunk::peerSize() const {
  return raft_node_.num_peers_;
}

inline void RaftChunk::syncLatestCommitTime(const Revision& item) {
  std::lock_guard<std::mutex> lock(latest_commit_time_mutex_);
  LogicalTime commit_time = item.getModificationTime();
  if (commit_time > latest_commit_time_) {
    latest_commit_time_ = commit_time;
  }
}

inline void RaftChunk::handleRaftConnectRequest(
    const PeerId& sender, proto::ConnectRequestType connect_type,
    Message* response) {
  CHECK_NOTNULL(response);
  if (raft_node_.isRunning()) {
    raft_node_.handleConnectRequest(sender, connect_type, response);
  } else {
    response->decline();
  }
}

inline void RaftChunk::handleRaftLeaveRequest(const PeerId& sender,
                                              uint64_t serial_id,
                                              Message* response) {
  CHECK_NOTNULL(response);
  if (raft_node_.isRunning() && raft_node_.hasPeer(sender)) {
    raft_node_.handleLeaveRequest(sender, serial_id, response);
  } else {
    response->decline();
  }
}

void RaftChunk::handleRaftChunkLockRequest(const PeerId& sender,
                                           uint64_t serial_id,
                                           Message* response) {
  CHECK_NOTNULL(response);
  if (raft_node_.isRunning() && raft_node_.hasPeer(sender)) {
    raft_node_.handleChunkLockRequest(sender, serial_id, response);
  } else {
    response->decline();
  }
}

void RaftChunk::handleRaftChunkUnlockRequest(const PeerId& sender,
                                             uint64_t serial_id,
                                             uint64_t lock_index,
                                             bool proceed_commits,
                                             Message* response) {
  CHECK_NOTNULL(response);
  if (raft_node_.isRunning() && raft_node_.hasPeer(sender)) {
    raft_node_.handleChunkUnlockRequest(sender, serial_id, lock_index,
                                        proceed_commits, response);
  } else {
    response->decline();
  }
}

inline void RaftChunk::handleRaftInsertRequest(proto::InsertRequest* request,
                                               const PeerId& sender,
                                               Message* response) {
  CHECK_NOTNULL(request);
  CHECK_NOTNULL(response);
  if (raft_node_.isRunning() && raft_node_.hasPeer(sender)) {
    raft_node_.handleInsertRequest(request, sender, response);
  } else {
    response->decline();
  }
}

inline void RaftChunk::handleRaftAppendRequest(
    proto::AppendEntriesRequest* request, const PeerId& sender,
    Message* response) {
  CHECK_NOTNULL(request);
  CHECK_NOTNULL(response);
  if (raft_node_.isRunning() && raft_node_.hasPeer(sender)) {
    raft_node_.handleAppendRequest(request, sender, response);
  } else {
    response->decline();
  }
}

inline void RaftChunk::handleRaftRequestVote(const proto::VoteRequest& request,
                                             const PeerId& sender,
                                             Message* response) {
  CHECK_NOTNULL(response);
  if (raft_node_.isRunning() && raft_node_.hasPeer(sender)) {
    raft_node_.handleRequestVote(request, sender, response);
  } else {
    response->decline();
  }
}

inline void RaftChunk::handleRaftQueryState(const proto::QueryState& request,
                                            Message* response) {
  CHECK_NOTNULL(response);
  if (raft_node_.isRunning()) {
    raft_node_.handleQueryState(request, response);
  } else {
    response->decline();
  }
}

inline void RaftChunk::handleRaftChunkTransactionInfo(
    proto::ChunkTransactionInfo* info, const PeerId& sender,
    Message* response) {
  if (raft_node_.isRunning()) {
    raft_node_.handleChunkTransactionInfo(info, sender, response);
  } else {
    response->decline();
  }
}

inline void RaftChunk::handleRaftQueryReadyToCommit(
    const proto::MultiChunkTransactionQuery& query, const PeerId& sender,
    Message* response) {
  if (raft_node_.isRunning()) {
    raft_node_.handleQueryReadyToCommit(query, sender, response);
  } else {
    response->decline();
  }
}

inline void RaftChunk::handleRaftCommitNotification(
    const proto::MultiChunkTransactionQuery& query, const PeerId& sender,
    Message* response) {
  if (raft_node_.isRunning()) {
    raft_node_.handleCommitNotification(query, sender, response);
  } else {
    response->decline();
  }
}

inline void RaftChunk::handleRaftAbortNotification(
    const proto::MultiChunkTransactionQuery& query, const PeerId& sender,
    Message* response) {
  if (raft_node_.isRunning()) {
    raft_node_.handleAbortNotification(query, sender, response);
  } else {
    response->decline();
  }
}

}  // namespace map_api

#endif  // MAP_API_RAFT_CHUNK_INL_H_
