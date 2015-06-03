#ifndef MAP_API_RAFT_NODE_INL_H_
#define MAP_API_RAFT_NODE_INL_H_

namespace map_api {

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

void RaftNode::setAppendEntriesResponse(proto::AppendEntriesResponse* response,
                                        proto::AppendResponseStatus status,
                                        uint64_t current_commit_index,
                                        uint64_t current_term,
                                        uint64_t last_log_index,
                                        uint64_t last_log_term) const {
  response->set_response(status);
  response->set_commit_index(current_commit_index);
  response->set_term(current_term);
  response->set_last_log_index(last_log_index);
  response->set_last_log_term(last_log_term);
}

template <typename RequestType>
void RaftNode::fillMetadata(RequestType* destination) const {
  CHECK_NOTNULL(destination);
  destination->mutable_metadata()->set_table(this->table_name_);
  this->chunk_id_.serialize(
      destination->mutable_metadata()->mutable_chunk_id());
}

}  // namespace map_api

#endif  // MAP_API_RAFT_NODE_INL_H_
