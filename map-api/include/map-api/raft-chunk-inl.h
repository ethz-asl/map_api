#ifndef MAP_API_RAFT_CHUNK_INL_H_
#define MAP_API_RAFT_CHUNK_INL_H_

void RaftChunk::setStateFollowerAndStartRaft() {
    raft_node_.state_ = RaftNode::State::FOLLOWER;
    VLOG(1) << PeerId::self() << ": Starting Raft node as follower for chunk "
            << id_.printString();
    raft_node_.start();
}

void RaftChunk::setStateLeaderAndStartRaft() {
    raft_node_.state_ = RaftNode::State::LEADER;
    VLOG(1) << PeerId::self() << ": Starting Raft node as leader for chunk "
            << id_.printString();
    raft_node_.start();
}

int RaftChunk::peerSize() const {
  return raft_node_.num_peers_;
}

inline void RaftChunk::syncLatestCommitTime(const Revision& item) {
  LogicalTime commit_time = item.getModificationTime();
  if (commit_time > latest_commit_time_) {
    latest_commit_time_ = commit_time;
  }
}

inline void RaftChunk::handleRaftConnectRequest(const PeerId& sender,
                                                Message* response) {
  raft_node_.handleConnectRequest(sender, response);
}

inline void RaftChunk::handleRaftAppendRequest(
    proto::AppendEntriesRequest* request, const PeerId& sender,
    Message* response) {
  raft_node_.handleAppendRequest(request, sender, response);
}

void RaftChunk::handleRaftChunkLockRequest(const PeerId& sender,
                                           Message* response) {
  raft_node_.handleChunkLockRequest(sender, response);
}

void RaftChunk::handleRaftChunkUnlockRequest(const PeerId& sender,
    uint64_t lock_index, bool proceed_commits, Message* response) {
  raft_node_.handleChunkUnlockRequest(sender, lock_index, proceed_commits,
                                      response);
}

inline void RaftChunk::handleRaftInsertRequest(proto::InsertRequest* request,
                                               const PeerId& sender,
                                               Message* response) {
  raft_node_.handleInsertRequest(request, sender, response);
}

inline void RaftChunk::handleRaftUpdateRequest(proto::InsertRequest* request,
                                               const PeerId& sender,
                                               Message* response) {
  raft_node_.handleUpdateRequest(request, sender, response);
}

inline void RaftChunk::handleRaftRequestVote(const proto::VoteRequest& request,
                                             const PeerId& sender,
                                             Message* response) {
  raft_node_.handleRequestVote(request, sender, response);
}

inline void RaftChunk::handleRaftQueryState(const proto::QueryState& request,
                                            Message* response) {
  raft_node_.handleQueryState(request, response);
}

inline void RaftChunk::handleRaftJoinQuitRequest(
    const proto::JoinQuitRequest& request, const PeerId& sender,
    Message* response) {
  raft_node_.handleJoinQuitRequest(request, sender, response);
}

inline void RaftChunk::handleRaftNotifyJoinQuitSuccess(
    const proto::NotifyJoinQuitSuccess& request, Message* response) {
  raft_node_.handleNotifyJoinQuitSuccess(request, response);
}

#endif  // MAP_API_RAFT_CHUNK_INL_H_
