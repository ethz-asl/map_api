#ifndef MAP_API_RAFT_CHUNK_INL_H_
#define MAP_API_RAFT_CHUNK_INL_H_

namespace map_api {

void RaftChunk::setStateFollowerAndStartRaft() {
    raft_node_.state_ = RaftNode::State::FOLLOWER;
    VLOG(1) << PeerId::self() << ": Starting Raft node as follower for chunk "
            << id_.printString();
    raft_node_.start();
}

int RaftChunk::peerSize() const {
  return raft_node_.num_peers_;
}

}  // namespace map_api

#endif  // MAP_API_RAFT_CHUNK_INL_H_
