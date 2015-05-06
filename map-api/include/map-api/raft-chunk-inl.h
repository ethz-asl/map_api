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

#endif  // MAP_API_RAFT_CHUNK_INL_H_
