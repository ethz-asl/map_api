#include "map-api/raft-node.h"

#include <algorithm>
#include <future>
#include <random>

#include <multiagent-mapping-common/condition.h>
#include <multiagent-mapping-common/conversions.h>
#include <multiagent-mapping-common/reader-writer-lock.h>

#include "./chunk.pb.h"
#include "./raft.pb.h"
#include "map-api/hub.h"
#include "map-api/logical-time.h"
#include "map-api/message.h"
#include "map-api/multi-chunk-commit.h"
#include "map-api/revision.h"

namespace map_api {

// TODO(aqurai): decide good values for these
constexpr int kHeartbeatTimeoutMs = 500;
constexpr int kHeartbeatSendPeriodMs = 200;
constexpr int kJoinResponseTimeoutMs = 1000;
// Maximum number of yet-to-be-committed entries allowed in the log.
constexpr int kMaxLogQueueLength = 50;

const char RaftNode::kAppendEntries[] = "raft_node_append_entries";
const char RaftNode::kAppendEntriesResponse[] = "raft_node_append_response";
const char RaftNode::kChunkLockRequest[] = "raft_node_chunk_lock_request";
const char RaftNode::kChunkUnlockRequest[] = "raft_node_chunk_unlock_request";
const char RaftNode::kChunkCommitInfo[] = "raft_node_chunk_commit_info";
const char RaftNode::kInsertRequest[] = "raft_node_insert_request";
const char RaftNode::kUpdateRequest[] = "raft_node_update_request";
const char RaftNode::kRaftChunkRequestResponse[] =
    "raft_node_chunk_request_response";
const char RaftNode::kVoteRequest[] = "raft_node_vote_request";
const char RaftNode::kVoteResponse[] = "raft_node_vote_response";
const char RaftNode::kLeaveRequest[] = "raft_node_leave_request";
const char RaftNode::kLeaveNotification[] = "raft_node_leave_notification";
const char RaftNode::kConnectRequest[] = "raft_node_connect_request";
const char RaftNode::kConnectResponse[] = "raft_node_connect_response";
const char RaftNode::kInitRequest[] = "raft_node_init_response";
const char RaftNode::kQueryState[] = "raft_node_query_state";
const char RaftNode::kQueryStateResponse[] = "raft_node_query_state_response";

MAP_API_PROTO_MESSAGE(RaftNode::kAppendEntries, proto::AppendEntriesRequest);
MAP_API_PROTO_MESSAGE(RaftNode::kAppendEntriesResponse,
                      proto::AppendEntriesResponse);
MAP_API_PROTO_MESSAGE(RaftNode::kChunkLockRequest, proto::LockRequest);
MAP_API_PROTO_MESSAGE(RaftNode::kChunkUnlockRequest, proto::UnlockRequest);
MAP_API_PROTO_MESSAGE(RaftNode::kChunkCommitInfo, proto::ChunkCommitInfo);
MAP_API_PROTO_MESSAGE(RaftNode::kInsertRequest, proto::InsertRequest);
MAP_API_PROTO_MESSAGE(RaftNode::kRaftChunkRequestResponse,
                      proto::RaftChunkRequestResponse);
MAP_API_PROTO_MESSAGE(RaftNode::kVoteRequest, proto::VoteRequest);
MAP_API_PROTO_MESSAGE(RaftNode::kVoteResponse, proto::VoteResponse);
MAP_API_PROTO_MESSAGE(RaftNode::kLeaveRequest, proto::RaftLeaveRequest);
MAP_API_PROTO_MESSAGE(RaftNode::kLeaveNotification,
                      proto::ChunkRequestMetadata);
MAP_API_PROTO_MESSAGE(RaftNode::kQueryState, proto::QueryState);
MAP_API_PROTO_MESSAGE(RaftNode::kQueryStateResponse, proto::QueryStateResponse);
MAP_API_PROTO_MESSAGE(RaftNode::kConnectRequest, proto::ChunkRequestMetadata);
MAP_API_PROTO_MESSAGE(RaftNode::kConnectResponse, proto::ConnectResponse);
MAP_API_PROTO_MESSAGE(RaftNode::kInitRequest, proto::InitRequest);

const PeerId kInvalidId = PeerId();

RaftNode::RaftNode()
    : leader_id_(PeerId()),
      state_(State::FOLLOWER),
      current_term_(0),
      last_heartbeat_(std::chrono::system_clock::now()),
      state_thread_running_(false),
      is_exiting_(false),
      leave_requested_(false),
      num_peers_(0),
      last_vote_request_term_(0) {
  election_timeout_ms_ = setElectionTimeout();
  VLOG(2) << "Peer " << PeerId::self()
          << ": Election timeout = " << election_timeout_ms_;
  data_ = new RaftChunkDataRamContainer;
  std::shared_ptr<proto::RaftLogEntry> default_entry(
      new proto::RaftLogEntry);
  default_entry->set_index(0);
  default_entry->set_term(0);
  LogWriteAccess log_writer(data_);
  log_writer->appendLogEntry(default_entry);
}

void RaftNode::start() {
  CHECK(chunk_id_.isValid());
  if (state_thread_running_) {
    LOG(FATAL) << "Start failed. State manager thread is already running.";
    return;
  }
  if (!is_exiting_) {
    state_manager_thread_ = std::thread(&RaftNode::stateManagerThread, this);
    // Avoid race conditions by waiting for the state thread to start.
    while (!state_thread_running_) {
      usleep(2000);
    }
  }
}

void RaftNode::stop() {
  if (state_thread_running_ && !is_exiting_) {
    is_exiting_ = true;
    giveUpLeadership();
    state_manager_thread_.join();
  }
  VLOG(1) << PeerId::self() << " is stopping raft node for chunk " << chunk_id_;
}

uint64_t RaftNode::getTerm() const {
  std::lock_guard<std::mutex> lock(state_mutex_);
  return current_term_;
}

const PeerId& RaftNode::getLeader() const {
  std::lock_guard<std::mutex> lock(state_mutex_);
  return leader_id_;
}

RaftNode::State RaftNode::getState() const {
  std::lock_guard<std::mutex> lock(state_mutex_);
  return state_;
}


// If there are no new entries, Leader sends empty message (heartbeat)
// Message contains leader commit index, used to update own commit index
// In Follower state, ONLY this thread writes to log_entries_ (via the function
//  followerAppendNewEntries(..))
void RaftNode::handleAppendRequest(proto::AppendEntriesRequest* append_request,
                                   const PeerId& sender, Message* response) {
  proto::AppendEntriesResponse append_response;
  VLOG(4) << "Received AppendRequest/Heartbeat from " << sender;

  const uint64_t request_term = append_request->term();

  // Lock and read the state.
  std::unique_lock<std::mutex> state_lock(state_mutex_);
  bool sender_changed = (sender != leader_id_ || request_term != current_term_);

  // Lock and read log info
  LogWriteAccess log_writer(data_);
  const uint64_t last_log_index = log_writer->lastLogIndex();
  const uint64_t last_log_term = log_writer->lastLogTerm();
  bool is_sender_log_newer =
      append_request->last_log_term() > last_log_term ||
      (append_request->last_log_term() == last_log_term &&
       append_request->last_log_index() >= last_log_index);

  // ================================
  // Check if the sender has changed.
  // ================================
  if (sender_changed) {
    if (request_term > current_term_ ||
        (request_term == current_term_ && !leader_id_.isValid()) ||
        (request_term < current_term_ && !leader_id_.isValid() &&
         is_sender_log_newer)) {
      // Update state and leader info if another leader with newer term is found
      // or, if a leader is found when a there isn't a known one. The new leader
      // should either have same/higher term or more updated log.
      current_term_ = request_term;
      leader_id_ = sender;
      if (state_ == State::LEADER || state_ == State::CANDIDATE) {
        state_ = State::FOLLOWER;
        follower_trackers_run_ = false;
      }
    } else if (state_ == State::FOLLOWER && request_term == current_term_ &&
               sender != leader_id_ && current_term_ > 0 &&
               leader_id_.isValid()) {
      // This should not happen.
      LOG(FATAL) << "Peer " << PeerId::self().ipPort()
                 << " has found 2 leaders in the same term (" << current_term_
                 << "). They are " << leader_id_.ipPort() << " (current) and "
                 << sender.ipPort() << " (new) ";
    } else {
      setAppendEntriesResponse(
          &append_response, proto::AppendResponseStatus::REJECTED,
          log_writer->commitIndex(), current_term_, log_writer->lastLogIndex(),
          log_writer->lastLogTerm());
      response->impose<kAppendEntriesResponse>(append_response);
      return;
    }
  }
  updateHeartbeatTime();

  // ==============================
  // Append/commit new log entries.
  // ==============================

  // Check if there are new entries.
  proto::AppendResponseStatus response_status =
      followerAppendNewEntries(log_writer, append_request);

  // Check if new entries are committed.
  if (response_status == proto::AppendResponseStatus::SUCCESS) {
    followerCommitNewEntries(log_writer, append_request->commit_index(),
                             state_);
  }

  setAppendEntriesResponse(
      &append_response, response_status, log_writer->commitIndex(),
      current_term_, log_writer->lastLogIndex(), log_writer->lastLogTerm());
  state_lock.unlock();
  response->impose<kAppendEntriesResponse>(append_response);
}

void RaftNode::handleRequestVote(const proto::VoteRequest& vote_request,
                                 const PeerId& sender, Message* response) {
  CHECK_NOTNULL(response);
  updateHeartbeatTime();
  proto::VoteResponse vote_response;
  std::lock_guard<std::mutex> state_lock(state_mutex_);
  bool is_candidate_log_newer;
  {
    LogReadAccess log_reader(data_);
    vote_response.set_previous_log_index(log_reader->lastLogIndex());
    vote_response.set_previous_log_term(log_reader->lastLogTerm());

    is_candidate_log_newer =
        vote_request.last_log_term() > log_reader->lastLogTerm() ||
        (vote_request.last_log_term() == log_reader->lastLogTerm() &&
         vote_request.last_log_index() >= log_reader->lastLogIndex());
  }
  last_vote_request_term_ =
    std::max(static_cast<uint64_t>(last_vote_request_term_), vote_request.term());
  if (state_ == State::JOINING || state_ == State::DISCONNECTING) {
    vote_response.set_vote(proto::VoteResponseType::VOTER_NOT_ELIGIBLE);
  } else if (vote_request.term() > current_term_ && is_candidate_log_newer) {
    vote_response.set_vote(proto::VoteResponseType::GRANTED);
    current_term_ = vote_request.term();
    leader_id_ = PeerId();
    if (state_ == State::LEADER) {
      follower_trackers_run_ = false;
    }
    state_ = State::FOLLOWER;
    VLOG(1) << "Peer " << PeerId::self().ipPort() << " is voting for " << sender
            << " in term " << current_term_ << " for chunk " << chunk_id_;
  } else {
    VLOG(1) << "Peer " << PeerId::self().ipPort() << " is declining vote for "
            << sender << " in term " << vote_request.term() << " for chunk "
            << chunk_id_ << ". Reason: " << (vote_request.term() > current_term_
                                                 ? ""
                                                 : "Term is equal or less. ")
            << (is_candidate_log_newer ? "" : "Log is older. ");
    vote_response.set_vote(proto::VoteResponseType::DECLINED);
  }

  response->impose<kVoteResponse>(vote_response);
  election_timeout_ms_ = setElectionTimeout();
}

void RaftNode::handleConnectRequest(const PeerId& sender, Message* response) {
  proto::ConnectResponse connect_response;
  uint64_t entry_index = 0;
  {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    if (state_ != State::LEADER) {
      connect_response.set_index(0);
      if (leader_id_.isValid()) {
        connect_response.set_leader_id(leader_id_.ipPort());
      }
    } else {
      LogWriteAccess log_writer(data_);
      std::lock_guard<std::mutex> tracker_lock(follower_tracker_mutex_);
      // TODO(aqurai): If the peer is already present, it could be re-joining.
      // In that case, avoid sending all log entries during connect.
      std::shared_ptr<proto::RaftLogEntry> new_entry(new proto::RaftLogEntry);
      new_entry->set_add_peer(sender.ipPort());
      entry_index =
          leaderAppendLogEntryLocked(log_writer, new_entry, current_term_);
      connect_response.set_index(entry_index);
    }
  }
  response->impose<kConnectResponse>(connect_response);
}

void RaftNode::handleLeaveRequest(const PeerId& sender, uint64_t serial_id,
                                  Message* response) {
  if (!checkReadyToHandleChunkRequests()) {
    response->decline();
    return;
  }
  CHECK(raft_chunk_lock_.holder().isValid());
  CHECK(sender.isValid());

  if (!raft_chunk_lock_.isLockHolder(sender)) {
    response->decline();
    return;
  }
  CHECK(raft_chunk_lock_.isLockHolder(sender));
  std::shared_ptr<proto::RaftLogEntry> entry(new proto::RaftLogEntry);
  entry->set_remove_peer(sender.ipPort());
  entry->set_sender(sender.ipPort());
  entry->set_sender_serial_id(serial_id);
  uint64_t index = leaderAppendLogEntry(entry);

  proto::RaftChunkRequestResponse leave_response;
  leave_response.set_entry_index(index);
  response->impose<kRaftChunkRequestResponse>(leave_response);
  leaderRemovePeer(sender);
}

void RaftNode::handleChunkLockRequest(const PeerId& sender, uint64_t serial_id,
                                      Message* response) {
  proto::RaftChunkRequestResponse lock_response =
      processChunkLockRequest(sender, serial_id, false);
  if (!hasPeer(sender)) {
    response->decline();
    return;
  }
  response->impose<kRaftChunkRequestResponse>(lock_response);
}

void RaftNode::handleChunkUnlockRequest(const PeerId& sender,
                                        uint64_t serial_id, uint64_t lock_index,
                                        bool proceed_commits,
                                        Message* response) {
  proto::RaftChunkRequestResponse unlock_response = processChunkUnlockRequest(
      sender, serial_id, false, lock_index, proceed_commits);
  response->impose<kRaftChunkRequestResponse>(unlock_response);
}

void RaftNode::handleInsertRequest(proto::InsertRequest* request,
                                   const PeerId& sender, Message* response) {
  proto::RaftChunkRequestResponse insert_response = processInsertRequest(
      sender, request->serial_id(), false, request->release_revision());
  response->impose<kRaftChunkRequestResponse>(insert_response);
}

void RaftNode::handleQueryState(const proto::QueryState& request,
                                Message* response) {
  proto::QueryStateResponse state_response;
  std::lock_guard<std::mutex> state_lock(state_mutex_);
  state_response.set_leader_id_(leader_id_.ipPort());

  LogReadAccess log_reader(data_);
  state_response.set_last_log_index(log_reader->lastLogIndex());
  state_response.set_last_log_term(log_reader->lastLogTerm());

  state_response.set_commit_index(log_reader->commitIndex());
  response->impose<kQueryStateResponse>(state_response);
}

bool RaftNode::checkReadyToHandleChunkRequests() const {
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

bool RaftNode::sendAppendEntries(
    const PeerId& peer, proto::AppendEntriesRequest* append_entries,
    proto::AppendEntriesResponse* append_response) {
  Message request, response;
  fillMetadata(append_entries);
  request.impose<kAppendEntries>(*append_entries);
  if (Hub::instance().try_request(peer, &request, &response)) {
    if (response.isType<kAppendEntriesResponse>()) {
      response.extract<kAppendEntriesResponse>(append_response);
      return true;
    } else {
      return false;
    }
  } else {
    VLOG(1) << "AppendEntries RPC failed for peer " << peer.ipPort();
    return false;
  }
}

RaftNode::VoteResponse RaftNode::sendRequestVote(
    const PeerId& peer, uint64_t term, uint64_t last_log_index,
    uint64_t last_log_term, uint64_t current_commit_index) const {
  Message request, response;
  proto::VoteRequest vote_request;
  vote_request.set_term(term);
  vote_request.set_commit_index(current_commit_index);
  vote_request.set_last_log_index(last_log_index);
  vote_request.set_last_log_term(last_log_term);
  fillMetadata(&vote_request);
  request.impose<kVoteRequest>(vote_request);
  if (Hub::instance().try_request(peer, &request, &response)) {
    proto::VoteResponse vote_response;
    if (!response.isType<kVoteResponse>()) {
      return VoteResponse::FAILED_REQUEST;
    }
    response.extract<kVoteResponse>(&vote_response);
    switch (vote_response.vote()) {
      case proto::VoteResponseType::GRANTED:
        return VoteResponse::VOTE_GRANTED;
      case proto::VoteResponseType::VOTER_NOT_ELIGIBLE:
        return VoteResponse::VOTER_NOT_ELIGIBLE;
      case proto::VoteResponseType::DECLINED:
        return VoteResponse::VOTE_DECLINED;
      default:
        return VoteResponse::VOTE_DECLINED;
    }
  } else {
    return VoteResponse::FAILED_REQUEST;
  }
}

// Peer lock acquired by the calling function, leaderAddPeer().
// Read lock for log acquired by leaderCommitReplicatedEntries().
bool RaftNode::sendInitRequest(const PeerId& peer,
                               const LogWriteAccess& log_writer) {
  Message request, response;
  proto::InitRequest init_request;
  fillMetadata(&init_request);
  for (const PeerId& chunk_peer : peer_list_) {
    init_request.add_peer_address(chunk_peer.ipPort());
  }
  init_request.add_peer_address(PeerId::self().ipPort());

  for (ConstLogIterator it = log_writer->cbegin(); it != log_writer->cend();
       ++it) {
    // Send only committed log entries.
    // Added advantage: Don't have to deal with revision separately for
    // committed and uncommitted entries.
    if ((*it)->index() > log_writer->commitIndex()) {
      break;
    }
    proto::RaftLogEntry entry(**it);
    CHECK(!entry.has_insert_revision());
    if ((*it)->has_revision_id()) {
      // Sending only committed entries, so revision will be in history, not
      // log.
      entry.set_allocated_insert_revision(CHECK_NOTNULL(
          data_->getByIdImpl(common::Id((*it)->revision_id()),
                             LogicalTime((*it)->logical_time())).get())
                                              ->underlying_revision_.get());
      entry.clear_revision_id();
      entry.clear_logical_time();
    }
    init_request.add_serialized_items(entry.SerializeAsString());
    entry.release_insert_revision();
  }
  request.impose<kInitRequest>(init_request);
  Hub::instance().try_request(peer, &request, &response);
  return response.isOk();
}

void RaftNode::stateManagerThread() {
  bool election_timeout = false;
  State state;
  uint64_t current_term;
  state_thread_running_ = true;

  while (!is_exiting_) {
    // Conduct election if timeout has occurred and the peer is not attempting
    // to leave chunk.
    if (election_timeout && !leave_requested_) {
      election_timeout = false;
      conductElection();
    }

    // Read state information.
    {
      std::lock_guard<std::mutex> state_lock(state_mutex_);
      state = state_;
      current_term = current_term_;
    }

    if (state == State::FOLLOWER) {
      if (getTimeSinceHeartbeatMs() > election_timeout_ms_) {
        VLOG(1) << "Follower: " << PeerId::self()
                << " : Heartbeat timed out for chunk " << chunk_id_;
        election_timeout = true;
      } else {
        usleep(election_timeout_ms_ * kMillisecondsToMicroseconds);
      }
    } else if (state == State::LEADER) {
      // Launch follower_handler threads if state is LEADER.
      follower_trackers_run_ = true;
      std::unique_lock<std::mutex> peer_lock(peer_mutex_);
      std::unique_lock<std::mutex> tracker_lock(follower_tracker_mutex_);
      for (const PeerId& peer : peer_list_) {
        leaderLaunchTracker(peer, current_term);
      }
      tracker_lock.unlock();
      peer_lock.unlock();

      while (follower_trackers_run_) {
        leaderCommitReplicatedEntries(current_term);
        leaderMonitorFollowerStatus(current_term);
        if (follower_trackers_run_) {
          std::mutex wait_mutex;
          std::unique_lock<std::mutex> wait_lock(wait_mutex);
          entry_replicated_signal_.wait_for(
              wait_lock, std::chrono::milliseconds(kHeartbeatSendPeriodMs));
        }
      }
      VLOG(1) << "Peer " << PeerId::self() << " Lost leadership. ";
      tracker_lock.lock();
      leaderShutDownAllTrackes();
      VLOG(1) << "Peer " << PeerId::self() << ": Follower trackers closed. ";
    }
  }  // while(!is_exiting_)
  VLOG(1) << PeerId::self() << ": Closing the State manager thread for chunk " << chunk_id_;
  state_thread_running_ = false;
}

bool RaftNode::hasPeer(const PeerId& peer) {
  std::lock_guard<std::mutex> peer_lock(peer_mutex_);
  return peer_list_.count(peer);
}

// Expects follower_tracker_mutex_ locked.
void RaftNode::leaderShutDownTracker(const PeerId& peer) {
  TrackerMap::iterator found = follower_tracker_map_.find(peer);
  if (found == follower_tracker_map_.end()) {
    return;
  }
  std::shared_ptr<FollowerTracker> tracker(found->second);
  tracker->tracker_run = false;
  tracker->tracker_thread.join();
  follower_tracker_map_.erase(peer);
}

void RaftNode::leaderShutDownAllTrackes() {
  follower_trackers_run_ = false;
  for (TrackerMap::value_type& tracker : follower_tracker_map_) {
    tracker.second->tracker_run = false;
    tracker.second->tracker_thread.join();
  }
  follower_tracker_map_.clear();
}

// Expects follower_tracker_mutex_ locked.
void RaftNode::leaderLaunchTracker(const PeerId& peer, uint64_t current_term) {
  std::shared_ptr<FollowerTracker> tracker;
  if (follower_tracker_map_.count(peer) == 1) {
    // Reconnect after disconnect.
    tracker = follower_tracker_map_.find(peer)->second;
    if (tracker->status == PeerStatus::AVAILABLE) {
      // Can happen if a peer is added but the peer doesn't know because it
      // didn't receive acknowledgement yet.
      LOG(WARNING) << "Peer " << peer
                   << " is already present but sent Join request again.";
      return;
    }
    tracker->status = PeerStatus::AVAILABLE;
    tracker->tracker_run = false;
    tracker->tracker_thread.join();
  } else {
    tracker = std::shared_ptr<FollowerTracker>(new FollowerTracker);
    follower_tracker_map_.insert(std::make_pair(peer, tracker));
    tracker->status = PeerStatus::AVAILABLE;
  }
  tracker->tracker_run = true;
  tracker->replication_index = 0;
  tracker->tracker_thread = std::thread(&RaftNode::followerTrackerThread, this,
                                        peer, current_term, tracker.get());
}

void RaftNode::leaderMonitorFollowerStatus(uint64_t current_term) {
  uint num_not_responding = 0;
  {
    LogWriteAccess log_writer(data_);
    std::unique_lock<std::mutex> peer_lock(peer_mutex_);
    std::unique_lock<std::mutex> tracker_lock(follower_tracker_mutex_);
    std::vector<PeerId> remove_peers;
    for (TrackerMap::value_type& tracker : follower_tracker_map_) {
      if (tracker.second->status == PeerStatus::OFFLINE) {
        ++num_not_responding;
      }
      if (tracker.second->status == PeerStatus::OFFLINE ||
          tracker.second->status == PeerStatus::ANNOUNCED_DISCONNECTING) {
        remove_peers.push_back(tracker.first);
      }
    }
    if (!remove_peers.empty()) {
      for (const PeerId& remove_peer : remove_peers) {
        VLOG(1) << remove_peer
                << " is offline. Shutting down the follower tracker.";
        leaderShutDownTracker(remove_peer);
        std::shared_ptr<proto::RaftLogEntry> new_entry(new proto::RaftLogEntry);
        new_entry->set_remove_peer(remove_peer.ipPort());
        leaderAppendLogEntryLocked(log_writer, new_entry, current_term);
        peer_list_.erase(remove_peer);
        num_peers_ = peer_list_.size();
      }
    }
  }

  // num_peers_ > 1 condition is needed to prevent the leader from thinking it
  // itself is disconnected when the last peer leaves (announced or sudden).
  // TODO(aqurai): What if there is one peer and leader disconnects?
  if (num_peers_ > 1 && num_not_responding > num_peers_ / 2) {
    VLOG(1) << PeerId::self()
            << ": Disconnected from network. Shutting down follower trackers. ";
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    state_ = State::LOST_CONNECTION;
    follower_trackers_run_ = false;
    return;
  }
}

void RaftNode::leaderAddPeer(const PeerId& peer,
                             const LogWriteAccess& log_writer,
                             uint64_t current_term) {
  std::lock_guard<std::mutex> peer_lock(peer_mutex_);
  std::lock_guard<std::mutex> tracker_lock(follower_tracker_mutex_);

  if (peer != PeerId::self() && peer_list_.count(peer) == 0) {  // Add new peer.
    sendInitRequest(peer, log_writer);
    peer_list_.insert(peer);
    num_peers_ = peer_list_.size();
    leaderLaunchTracker(peer, current_term);
    VLOG(1) << "Leader has added peer " << peer << " to chunk " << chunk_id_
            << ". Raft group size: " << num_peers_ + 1;
  }
}

void RaftNode::leaderRemovePeer(const PeerId& peer) {
  std::lock_guard<std::mutex> peer_lock(peer_mutex_);
  std::lock_guard<std::mutex> tracker_lock(follower_tracker_mutex_);

  peer_list_.erase(peer);
  num_peers_ = peer_list_.size();
  leaderShutDownTracker(peer);
  VLOG(1) << "Leader has removed peer " << peer << " from chunk " << chunk_id_
          << ". Raft group size: " << num_peers_ + 1;
}

void RaftNode::followerAddPeer(const PeerId& peer) {
  std::lock_guard<std::mutex> peer_lock(peer_mutex_);
  if (peer != PeerId::self()) {
    peer_list_.insert(peer);
    num_peers_ = peer_list_.size();
  }
}

void RaftNode::followerRemovePeer(const PeerId& peer) {
  std::lock_guard<std::mutex> peer_lock(peer_mutex_);
  peer_list_.erase(peer);
  num_peers_ = peer_list_.size();
}

void RaftNode::conductElection() {
  uint num_votes = 0;
  uint num_failed = 0;
  uint num_ineligible = 0;
  std::unique_lock<std::mutex> state_lock(state_mutex_);
  state_ = State::CANDIDATE;
  uint64_t old_term = current_term_;
  current_term_ = std::max(current_term_ + 1, last_vote_request_term_ + 1);
  uint64_t term = current_term_;
  leader_id_ = PeerId();
  LogReadAccess log_reader(data_);
  const uint64_t last_log_index = log_reader->lastLogIndex();
  const uint64_t last_log_term = log_reader->lastLogTerm();
  const uint64_t current_commit_index = log_reader->commitIndex();

  log_reader.unlockAndDisable();
  state_lock.unlock();

  VLOG(1) << "Peer " << PeerId::self() << " is an election candidate for term "
          << term << " for the chunk " << chunk_id_;

  std::vector<std::future<VoteResponse>> responses;

  {
    std::lock_guard<std::mutex> peer_lock(peer_mutex_);
    for (const PeerId& peer : peer_list_) {
      std::future<VoteResponse> vote_response =
          std::async(std::launch::async, &RaftNode::sendRequestVote, this, peer,
                     term, last_log_index, last_log_term, current_commit_index);
      responses.push_back(std::move(vote_response));
    }
  }

  for (std::future<VoteResponse>& response : responses) {
    VoteResponse vote_response = response.get();
    if (vote_response == VoteResponse::VOTE_GRANTED) {
      ++num_votes;
    } else if (vote_response == VoteResponse::VOTE_GRANTED) {
      ++num_ineligible;
    } else if (vote_response == VoteResponse::FAILED_REQUEST) {
      ++num_failed;
    }
  }

  state_lock.lock();
  if (num_failed > num_peers_ / 2) {
    state_ = State::LOST_CONNECTION;
    current_term_ = old_term;
    return;
  } else if (state_ == State::CANDIDATE &&
             num_votes + 1 >
                 (num_peers_ + 1 - num_failed - num_ineligible) / 2) {
    // This peer wins the election.
    state_ = State::LEADER;
    leader_id_ = PeerId::self();
    // Renew election timeout every session.
    election_timeout_ms_ = setElectionTimeout();

    // Append a blank entry to log to mark leader change.
    std::shared_ptr<proto::RaftLogEntry> default_entry(new proto::RaftLogEntry);
    LogWriteAccess log_writer(data_);
    leaderAppendLogEntryLocked(log_writer, default_entry, current_term_);
    VLOG(1) << "*** Peer " << PeerId::self()
            << " Elected as the leader of chunk " << chunk_id_ << " for term "
            << current_term_ << " with " << num_votes + 1 << " votes. ***";
  } else if (state_ == State::CANDIDATE) {
    // This peer doesn't win the election.
    state_ = State::FOLLOWER;
    leader_id_ = PeerId();
    // Set a longer election timeout if the candidate loses election to prevent
    // from holding elections and getting rejected repeatedly in consecutive
    // terms (due to less updated log) and blocking other peers from holding
    // election.
    election_timeout_ms_ = 4 * setElectionTimeout();
  }
  updateHeartbeatTime();
}

void RaftNode::initChunkData(const proto::InitRequest& init_request) {
  LogWriteAccess log_writer(data_);
  std::lock_guard<std::mutex> peer_lock(peer_mutex_);
  peer_list_.clear();
  log_writer->clear();
  for (int i = 0; i < init_request.peer_address_size(); ++i) {
    peer_list_.insert(PeerId(init_request.peer_address(i)));
  }
  num_peers_ = peer_list_.size();
  for (int i = 0; i < init_request.serialized_items_size(); ++i) {
    std::shared_ptr<proto::RaftLogEntry> entry(new proto::RaftLogEntry);
    entry->ParseFromString(init_request.serialized_items(i));
    log_writer->appendLogEntry(entry);
    followerCommitNewEntries(log_writer, log_writer->lastLogIndex(),
                             State::JOINING);
  }
  VLOG(3) << PeerId::self() << ": Initialized data and ready to join chunk "
          << chunk_id_.printString();
}

void RaftNode::followerTrackerThread(const PeerId& peer, uint64_t term,
                                     FollowerTracker* const this_tracker) {
  uint64_t follower_next_index =
      data_->logCommitIndex() + 1;  // This is at least 1.
  uint64_t follower_commit_index = 0;
  proto::AppendEntriesRequest append_entries;
  proto::AppendEntriesResponse append_response;

  // This lock is only used for waiting on a condition variable
  // (new_entries_signal_), which is notified when there is a new log entry.
  std::mutex wait_mutex;
  std::unique_lock<std::mutex> wait_lock(wait_mutex);

  while (follower_trackers_run_ && this_tracker->tracker_run) {
    bool append_success = false;
    while (!append_success && follower_trackers_run_ &&
           this_tracker->tracker_run) {
      if (this_tracker->status == PeerStatus::OFFLINE) {
        VLOG_EVERY_N(1, 10)
            << "Peer is offline. Not calling sendAppendEntries.";
        usleep(kJoinResponseTimeoutMs);
        continue;
      }
      bool sending_entries = false;
      append_entries.Clear();
      append_response.Clear();
      append_entries.set_term(term);
      uint64_t last_log_index;
      {
        LogWriteAccess log_reader(data_);
        append_entries.set_commit_index(log_reader->commitIndex());
        last_log_index = log_reader->lastLogIndex();
        append_entries.set_last_log_index(log_reader->lastLogIndex());
        append_entries.set_last_log_term(log_reader->lastLogTerm());
        if (follower_next_index <= log_reader->lastLogIndex()) {
          sending_entries = true;
          // There is at least one new entry to be sent.
          ConstLogIterator it =
              log_reader->getConstLogIteratorByIndex(follower_next_index);

          // if this is the case, the control shouldn't have reached here,
          CHECK(it != log_reader->cend());
          CHECK(it != log_reader->cbegin());

          proto::RaftLogEntry* log_entry = log_reader->copyWithoutRevision(it);
          if ((*it)->has_revision_id()) {
            log_entry->set_allocated_insert_revision(CHECK_NOTNULL(
                data_->getByIdImpl(common::Id((*it)->revision_id()),
                                   LogicalTime((*it)->logical_time()))
                    .get())->underlying_revision_.get());
          } else if ((*it)->has_insert_revision()) {
            log_entry->set_allocated_insert_revision(
                (*it)->mutable_insert_revision());
          }
          append_entries.set_allocated_log_entry(log_entry);
          append_entries.set_previous_log_index((*(it - 1))->index());
          append_entries.set_previous_log_term((*(it - 1))->term());
        }
      }

      if (!sendAppendEntries(peer, &append_entries, &append_response)) {
        if (this_tracker->status == PeerStatus::AVAILABLE) {
          this_tracker->status = PeerStatus::OFFLINE;
          VLOG(1) << PeerId::self() << ": Failed sendAppendEntries to " << peer;
        } else {
          this_tracker->status = PeerStatus::OFFLINE;
          // this_tracker->tracker_run = false;
          VLOG(1) << PeerId::self() << ": " << peer << " appears offline.";
        }
        append_entries.mutable_log_entry()->release_insert_revision();
        continue;
      }
      this_tracker->status = PeerStatus::AVAILABLE;
      // The call to release is necessary to prevent prevent the allocated
      // memory being deleted during append_entries.Clear().
      append_entries.mutable_log_entry()->release_insert_revision();

      follower_commit_index = append_response.commit_index();
      if (append_response.response() == proto::AppendResponseStatus::SUCCESS ||
           append_response.response() ==
               proto::AppendResponseStatus::ALREADY_PRESENT) {
        if (sending_entries) {
          // The response is from an append entry RPC, not a regular heartbeat.
          this_tracker->replication_index.store(follower_next_index);
          ++follower_next_index;
          entry_replicated_signal_.notify_all();
          append_success = (follower_next_index > last_log_index);
        }
      } else if (append_response.response() ==
                 proto::AppendResponseStatus::REJECTED) {
        // TODO(aqurai): Handle this.
      } else {
        // Append on follower failed due to a conflict. Send an older entry
        // and try again.
        CHECK_GT(follower_next_index, 1);
        follower_next_index = std::min(follower_next_index - 1,
                                       append_response.last_log_index() + 1);
        if (follower_commit_index >= follower_next_index &&
            (append_response.response() !=
             proto::AppendResponseStatus::REJECTED)) {
          // This should not happen.
          LOG(FATAL) << PeerId::self()
                     << ": Conflicting entry already committed on peer " << peer
                     << ". Peer commit index " << follower_commit_index
                     << ". Peer last log index, term "
                     << append_response.last_log_index() << ", "
                     << append_response.last_log_term() << ". "
                     << "Leader previous log index, term "
                     << append_entries.previous_log_index() << ", "
                     << append_entries.previous_log_term() << ". ";
        }
      }
    }  //  while (!append_successs && follower_trackers_run_)

    if (follower_trackers_run_ && this_tracker->tracker_run) {
      new_entries_signal_.wait_for(
          wait_lock, std::chrono::milliseconds(kHeartbeatSendPeriodMs));
    }
  }  // while (follower_trackers_run_)
}

int RaftNode::setElectionTimeout() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(kHeartbeatTimeoutMs,
                                       3 * kHeartbeatTimeoutMs);
  return dist(gen);
}

proto::AppendResponseStatus RaftNode::followerAppendNewEntries(
    const LogWriteAccess& log_writer, proto::AppendEntriesRequest* request) {
  if (!request->has_log_entry()) {
    // No need to proceed further as message contains no new entries
    if (request->last_log_index() == log_writer->lastLogIndex() &&
        request->last_log_term() == log_writer->lastLogTerm()) {
      return proto::AppendResponseStatus::SUCCESS;
    } else {
      return proto::AppendResponseStatus::FAILED;
    }
  } else if (request->previous_log_index() == log_writer->lastLogIndex() &&
             request->previous_log_term() == log_writer->lastLogTerm()) {
    // All is well, append the new entry, but don't commit it yet.
    std::shared_ptr<proto::RaftLogEntry> new_log_entry(
        request->release_log_entry());
    CHECK_EQ(new_log_entry->index(), log_writer->lastLogIndex() + 1);
    log_writer->appendLogEntry(new_log_entry);
    return proto::AppendResponseStatus::SUCCESS;
  } else if (request->previous_log_index() < log_writer->lastLogIndex()) {
    // Leader sends an older entry due to a conflict
    LogIterator it =
        log_writer->getLogIteratorByIndex(request->previous_log_index());
    if (it != log_writer->end() &&
        request->previous_log_term() == (*it)->term()) {
      // The received entry matched one of the older entries in the log.
      CHECK(it != log_writer->end());
      CHECK((it + 1) != log_writer->end());
      // Erase and replace only of the entry is different from the one already
      // stored.
      if ((*(it + 1))->term() == request->log_entry().term()) {
        return proto::AppendResponseStatus::ALREADY_PRESENT;
      } else {
        VLOG(1) << "Leader is erasing entries in log of " << PeerId::self()
                << ". from " << (*(it + 1))->index();
        CHECK_LT(log_writer->commitIndex(), (*(it + 1))->index());
        log_writer->resize(std::distance(log_writer->begin(), it + 1));
        std::shared_ptr<proto::RaftLogEntry> new_log_entry(
            request->release_log_entry());
        CHECK_EQ(new_log_entry->index(), log_writer->lastLogIndex() + 1);
        log_writer->appendLogEntry(new_log_entry);
        return proto::AppendResponseStatus::SUCCESS;
      }
    } else {
      return proto::AppendResponseStatus::FAILED;
    }
  } else {
    // term and index don't match with leader's log.
    return proto::AppendResponseStatus::FAILED;
  }
  return proto::AppendResponseStatus::FAILED;
}

void RaftNode::followerCommitNewEntries(const LogWriteAccess& log_writer,
                                        uint64_t request_commit_index,
                                        State state) {
  CHECK_LE(log_writer->commitIndex(), log_writer->lastLogIndex());
  if (log_writer->commitIndex() < request_commit_index &&
      log_writer->commitIndex() < log_writer->lastLogIndex()) {
    LogIterator old_commit =
        log_writer->getLogIteratorByIndex(log_writer->commitIndex());

    uint64_t new_commit_index =
        std::min(log_writer->lastLogIndex(), request_commit_index);
    LogIterator new_commit =
        log_writer->getLogIteratorByIndex(new_commit_index);

    std::for_each(old_commit + 1, new_commit + 1,
                  [&](const std::shared_ptr<proto::RaftLogEntry>& entry) {
      // Joining peers don't act on add/remove peer entries.
      if (state == State::FOLLOWER && entry->has_add_peer()) {
        followerAddPeer(PeerId(entry->add_peer()));
      }
      if (state == State::FOLLOWER && entry->has_remove_peer()) {
        if (entry->has_sender() && entry->sender() == entry->remove_peer()) {
          CHECK(raft_chunk_lock_.isLockHolder(PeerId(entry->remove_peer())));
        }
        if (raft_chunk_lock_.isLockHolder(PeerId(entry->remove_peer()))) {
          CHECK(raft_chunk_lock_.unlock());
        }
        followerRemovePeer(PeerId(entry->remove_peer()));
      }
      if (!raft_chunk_lock_.isLocked()) {
        // TODO(aqurai): Ensure all revision commits are locked.
        applySingleRevisionCommit(entry);
      }
      chunkLockEntryCommit(log_writer, entry);
    });
    log_writer->setCommitIndex(new_commit_index);
    entry_committed_signal_.notify_all();
    // TODO(aqurai): remove log later. Or increase the verbosity arg.
    VLOG_EVERY_N(2, 50) << PeerId::self() << ": Entry "
                        << log_writer->commitIndex() << " committed *****";
  }
}

uint64_t RaftNode::leaderAppendLogEntry(
    const std::shared_ptr<proto::RaftLogEntry>& entry) {
  uint64_t current_term;
  {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    if (state_ != State::LEADER) {
      return 0;
    }
    current_term = current_term_;
  }
  LogWriteAccess log_writer(data_);
  if (log_writer->lastLogIndex() - log_writer->commitIndex() >
      kMaxLogQueueLength) {
    return 0;
  }
  // Check if this entry is already in the log. If so, return its index.
  if (entry->has_sender() &&
      log_writer->getPeerLatestSerialId(PeerId(entry->sender())) >=
          entry->sender_serial_id()) {
    uint64_t index = log_writer->getEntryIndex(PeerId(entry->sender()),
                                               entry->sender_serial_id());
    LOG(WARNING) << "Entry already present at index " << index
                 << ". Sender: " << entry->sender()
                 << ". Entry serial id: " << entry->sender_serial_id()
                 << " in chunk " << chunk_id_ << ". "
                 << getLogEntryTypeString(entry);
    return index;
  }
  return leaderAppendLogEntryLocked(log_writer, entry, current_term);
}

uint64_t RaftNode::leaderAppendLogEntryLocked(
    const LogWriteAccess& log_writer,
    const std::shared_ptr<proto::RaftLogEntry>& new_entry,
    uint64_t current_term) {
  new_entry->set_index(log_writer->lastLogIndex() + 1);
  new_entry->set_term(current_term);
  log_writer->appendLogEntry(new_entry);
  new_entries_signal_.notify_all();
  VLOG_EVERY_N(1, 10) << "Adding entry to log with index "
                      << new_entry->index();
  return new_entry->index();
}

void RaftNode::leaderCommitReplicatedEntries(uint64_t current_term) {
  LogWriteAccess log_writer(data_);
  uint replication_count = 0;
  {
    std::lock_guard<std::mutex> tracker_lock(follower_tracker_mutex_);
    for (TrackerMap::value_type& tracker : follower_tracker_map_) {
      if (tracker.second->replication_index.load() >
          log_writer->commitIndex()) {
        ++replication_count;
      }
    }
  }

  if (replication_count > num_peers_) {
    LOG(FATAL) << "Replication count (" << replication_count
               << ") is higher than peer size (" << num_peers_ << ") at peer "
               << PeerId::self() << " for entry index "
               << log_writer->commitIndex() + 1;
  }

  ConstLogIterator it =
      log_writer->getConstLogIteratorByIndex(log_writer->commitIndex() + 1);
  if (it == log_writer->cend()) {
    return;
  }
  CHECK_LE(log_writer->commitIndex() + 1, log_writer->lastLogIndex());

  // Commit entries from older leaders only if they are replicated on all peers,
  // because otherwise they can potentially be overwritten by new leaders.
  // (see §5.4.2 of http://ramcloud.stanford.edu/raft.pdf)
  bool ready_to_commit =
      ((*it)->term() < current_term && replication_count == num_peers_) ||
      ((*it)->term() == current_term && replication_count > num_peers_ / 2) ||
      num_peers_ == 0;

  if (ready_to_commit) {
    if ((*it)->has_add_peer()) {
      leaderAddPeer(PeerId((*it)->add_peer()), log_writer, current_term);
    }
    if ((*it)->has_remove_peer()) {
      // Remove request can be sent by a leaving peer or when leader detects a
      // non responding peer.
      if ((*it)->has_sender() && (*it)->sender() == (*it)->remove_peer()) {
        CHECK(raft_chunk_lock_.isLockHolder(PeerId((*it)->remove_peer())));
      }
      if (raft_chunk_lock_.isLockHolder(PeerId((*it)->remove_peer()))) {
        CHECK(raft_chunk_lock_.unlock());
      }
      leaderRemovePeer(PeerId((*it)->remove_peer()));
    }
    if (!raft_chunk_lock_.isLocked()) {
      // TODO(aqurai): Obsolete?
      applySingleRevisionCommit(*it);
    }
    chunkLockEntryCommit(log_writer, *it);
    multiChunkCommitInfoCommit(*it);
    log_writer->setCommitIndex(log_writer->commitIndex() + 1);
    entry_committed_signal_.notify_all();
    VLOG_EVERY_N(2, 10) << PeerId::self() << ": Commit index increased to "
                        << log_writer->commitIndex()
                        << " With replication count " << replication_count
                        << " and term " << (*it)->term();

    // TODO(aqurai): Send notification to quitting peers after zerommq crash
    // issue is resolved.
    // There is a crash if one attempts to send message to a peer after a
    // previous message to the same peer timed out. So if a peer is removed
    // because it was not responding, sending a notification will cause a
    // crash. Otherwise, here I have to make a distinction between announced
    // and unannounced peer removal. Also, There could be other
    // threads/methods (eg: vote request) attempting to send message to the
    // non responsive peer, which causes a problem.
  }
}

void RaftNode::applySingleRevisionCommit(
    const std::shared_ptr<proto::RaftLogEntry>& entry) {
  // TODO(aqurai): Ensure all revision commits are locked.
  if (entry->has_insert_revision()) {
    const std::shared_ptr<Revision> insert_revision = Revision::fromProto(
        std::unique_ptr<proto::Revision>(entry->release_insert_revision()));
    insert_revision->getId<common::Id>().serialize(
        entry->mutable_revision_id());
    entry->set_logical_time(insert_revision->getUpdateTime().serialize());
    data_->checkAndPatch(insert_revision);
  }
}

void RaftNode::chunkLockEntryCommit(const LogWriteAccess& log_writer,
    const std::shared_ptr<proto::RaftLogEntry>& entry) {
  if (entry->has_lock_peer()) {
    raft_chunk_lock_.writeLock(PeerId(entry->lock_peer()), entry->index());
  }
  if (entry->has_unlock_peer()) {
    if (raft_chunk_lock_.isLockHolder(PeerId(entry->unlock_peer()))) {
      if (entry->unlock_proceed_commits()) {
        bulkApplyLockedRevisions(log_writer,
            raft_chunk_lock_.lock_entry_index(), entry->index());
      }
      raft_chunk_lock_.unlock();
    }
  }
}

void RaftNode::multiChunkCommitInfoCommit(
    const std::shared_ptr<proto::RaftLogEntry>& entry) {
  if (entry->has_multi_chunk_commit_info()) {
    multi_chunk_commit_manager_->initMultiChunkCommit(
        entry->multi_chunk_commit_info(),
        entry->multi_chunk_transaction_num_entries());
  }
}

void RaftNode::bulkApplyLockedRevisions(const LogWriteAccess& log_writer,
    uint64_t lock_index, uint64_t unlock_index) {
  LogIterator it_end = log_writer->getLogIteratorByIndex(unlock_index);
  for (LogIterator it = log_writer->getLogIteratorByIndex(lock_index+1);
       it != log_writer->end() && it != it_end; ++it) {
    applySingleRevisionCommit(*it);
  }
}

bool RaftNode::giveUpLeadership() {
  std::unique_lock<std::mutex> lock(state_mutex_);
  if (state_ == State::LEADER) {
    follower_trackers_run_ = false;
    state_ = State::FOLLOWER;
    leader_id_ = PeerId();
    std::lock_guard<std::mutex> follower_tracker_lock(follower_tracker_mutex_);
    leaderShutDownAllTrackes();
    lock.unlock();

    updateHeartbeatTime();
    // Prevent this peer from becoming leader again.
    election_timeout_ms_ = 4 * setElectionTimeout();
    return true;
  } else {
    lock.unlock();
    return false;
  }
}

void RaftNode::initializeMultiChunkCommitManager() {
  CHECK(chunk_id_.isValid());
  multi_chunk_commit_manager_.reset(new MultiChunkCommit(chunk_id_));
}

bool RaftNode::DistributedRaftChunkLock::writeLock(const PeerId& peer,
                                                   uint64_t index) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (is_locked_) {
    // LOG(ERROR) << "Lock false for " << peer << " at log index " << index;
    return false;
  }
  CHECK(peer.isValid());
  CHECK_GT(index, 0);
  is_locked_ = true;
  holder_ = peer;
  lock_entry_index_ = index;
  return true;
}

bool RaftNode::DistributedRaftChunkLock::unlock() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!is_locked_) {
    LOG(ERROR) << "Unlock false";
    return false;
  }
  is_locked_ = false;
  lock_entry_index_ = 0;
  holder_ = PeerId();
  return true;
}

uint64_t RaftNode::DistributedRaftChunkLock::lock_entry_index() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return lock_entry_index_;
}

bool RaftNode::DistributedRaftChunkLock::isLocked() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return is_locked_;
}

const PeerId& RaftNode::DistributedRaftChunkLock::holder() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return holder_;
}

bool RaftNode::DistributedRaftChunkLock::isLockHolder(const PeerId& peer)
    const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!holder_.isValid() || !peer.isValid()) {
    return false;
  }
  return (holder_ == peer);
}

uint64_t RaftNode::sendChunkLockRequest(uint64_t serial_id) {
  State append_state;
  PeerId leader_id;
  uint64_t append_term;
  uint64_t index = 0;
  {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    append_state = state_;
    leader_id = leader_id_;
    append_term = current_term_;
  }
  if (append_state == State::LEADER) {
    index =
        processChunkLockRequest(PeerId::self(), serial_id, true).entry_index();
  } else if (append_state == State::FOLLOWER && leader_id.isValid()) {
    // LOG(WARNING) <<PeerId::self() << "Sending lock req to " << leader_id;
    Message request, response;
    proto::LockRequest lock_request;
    proto::RaftChunkRequestResponse lock_response;
    fillMetadata(&lock_request);
    lock_request.set_serial_id(serial_id);
    request.impose<kChunkLockRequest>(lock_request);
    if (!Hub::instance().try_request(leader_id, &request, &response)) {
      VLOG(1) << "Lock request RPC failed.";
      return 0;
    }
    if (response.isType<kRaftChunkRequestResponse>()) {
      response.extract<kRaftChunkRequestResponse>(&lock_response);
      index = lock_response.entry_index();
    }
  }
  if (index > 0 && waitAndCheckCommit(index, append_term, serial_id)) {
    return index;
  }
  return 0;
}

uint64_t RaftNode::sendChunkUnlockRequest(uint64_t serial_id,
                                          uint64_t lock_index,
                                          bool proceed_commits) {
  State append_state;
  PeerId leader_id;
  uint64_t append_term;
  uint64_t index = 0;
  {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    append_state = state_;
    leader_id = leader_id_;
    append_term = current_term_;
  }
  if (append_state == State::LEADER) {
    index =
        processChunkUnlockRequest(PeerId::self(), serial_id, false, lock_index,
                                  proceed_commits).entry_index();
  } else if (append_state == State::FOLLOWER && leader_id.isValid()) {
    Message request, response;
    proto::UnlockRequest unlock_request;
    fillMetadata(&unlock_request);
    unlock_request.set_lock_entry_index(lock_index);
    unlock_request.set_proceed_commits(proceed_commits);
    unlock_request.set_serial_id(serial_id);
    request.impose<kChunkUnlockRequest>(unlock_request);
    if (!Hub::instance().try_request(leader_id, &request, &response)) {
      VLOG(1) << "Update request failed.";
      return 0;
    }
    if (response.isType<kRaftChunkRequestResponse>()) {
      proto::RaftChunkRequestResponse unlock_response;
      response.extract<kRaftChunkRequestResponse>(&unlock_response);
      index = unlock_response.entry_index();
    }
  }
  if (index > 0 && waitAndCheckCommit(index, append_term, serial_id)) {
    return index;
  }
  return 0;
}

uint64_t RaftNode::sendChunkCommitInfo(proto::ChunkCommitInfo info,
                                       uint64_t serial_id) {
  State append_state;
  PeerId leader_id;
  uint64_t append_term;
  uint64_t index = 0;
  {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    append_state = state_;
    leader_id = leader_id_;
    append_term = current_term_;
  }
  if (append_state == State::LEADER) {
    proto::ChunkCommitInfo* unowned_info_ptr = new proto::ChunkCommitInfo;
    unowned_info_ptr->CopyFrom(info);
    index = processChunkCommitInfo(PeerId::self(), serial_id, unowned_info_ptr)
                .entry_index();
  } else if (append_state == State::FOLLOWER && leader_id.isValid()) {
    Message request, response;
    fillMetadata(&info);
    info.set_serial_id(serial_id);
    proto::RaftChunkRequestResponse chunk_response;
    request.impose<kChunkCommitInfo>(info);
    if (!Hub::instance().try_request(leader_id, &request, &response)) {
      VLOG(1) << "Send Chunk commit info failed.";
      return 0;
    }
    if (response.isType<kRaftChunkRequestResponse>()) {
      response.extract<kRaftChunkRequestResponse>(&chunk_response);
      index = chunk_response.entry_index();
    }
  }
  if (index > 0 && waitAndCheckCommit(index, append_term, serial_id)) {
    return index;
  }
  return 0;
}

uint64_t RaftNode::sendInsertRequest(const Revision::ConstPtr& item,
                                     uint64_t serial_id,
                                     bool is_retry_attempt) {
  State append_state;
  PeerId leader_id;
  uint64_t append_term;
  uint64_t index = 0;
  {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    append_state = state_;
    leader_id = leader_id_;
    append_term = current_term_;
  }
  if (append_state == State::LEADER) {
    index = processInsertRequest(PeerId::self(), serial_id, is_retry_attempt,
                                 item->copyToProtoPtr()).entry_index();
  } else if (append_state == State::FOLLOWER && leader_id.isValid()) {
    Message request, response;
    proto::InsertRequest insert_request;
    proto::RaftChunkRequestResponse insert_response;
    fillMetadata(&insert_request);
    insert_request.set_serial_id(serial_id);
    insert_request.set_is_retry_attempt(is_retry_attempt);
    insert_request.set_allocated_revision(item->copyToProtoPtr());
    request.impose<kInsertRequest>(insert_request);
    if (!Hub::instance().try_request(leader_id, &request, &response)) {
      VLOG(1) << "Insert request might have failed.";
      return 0;
    }
    if (response.isType<kRaftChunkRequestResponse>()) {
      response.extract<kRaftChunkRequestResponse>(&insert_response);
      index = insert_response.entry_index();
    }
  }
  if (index > 0 && waitAndCheckCommit(index, append_term, serial_id)) {
    return index;
  }
  return 0;
}

bool RaftNode::waitAndCheckCommit(uint64_t index, uint64_t append_term,
                                  uint64_t serial_id) {
  while (isRunning()) {
    if (data_->logCommitIndex() >= index) {
      break;
    } else if (data_->lastLogTerm() > append_term) {
      // Term changed before our entry could be appended.
      return false;
    }
    std::mutex wait_mutex;
    std::unique_lock<std::mutex> wait_lock(wait_mutex);
    // Wait time limit to avoid deadlocks.
    entry_committed_signal_.wait_for(
        wait_lock, std::chrono::milliseconds(kHeartbeatSendPeriodMs));
  }
  if (!isRunning()) {
    return false;
  }
  LogReadAccess log_reader(data_);
  ConstLogIterator it = log_reader->getConstLogIteratorByIndex(index);
  if ((*it)->has_sender()) {
    if (PeerId((*it)->sender()) == PeerId::self() &&
        (*it)->sender_serial_id() == serial_id) {
      return true;
    }
  }
  return false;
}

bool RaftNode::sendLeaveRequest(uint64_t serial_id) {
  leave_requested_ = true;
  State append_state;
  uint64_t append_term;
  PeerId leader_id;
  {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    append_state = state_;
    append_term = current_term_;
    leader_id = leader_id_;
  }
  if (append_state == State::LEADER && num_peers_ > 0) {
    if (!checkReadyToHandleChunkRequests()) {
      return false;
    }
    waitAndCheckCommit(data_->lastLogIndex(), append_term, 0);
    giveUpLeadership();
    while (num_peers_ > 0 && isRunning()) {
      {
        std::lock_guard<std::mutex> state_lock(state_mutex_);
        if (leader_id_.isValid() && leader_id_ != PeerId::self() &&
            state_ == State::FOLLOWER) {
          CHECK_NE(current_term_, append_term);
          append_state = state_;
          leader_id = leader_id_;
          break;
        }
      }
      usleep(kHeartbeatTimeoutMs * kMillisecondsToMicroseconds);
    }
  }
  if (num_peers_ == 0) {
    return true;
  }
  {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    state_ = State::DISCONNECTING;
  }
  Message request, response;
  proto::RaftLeaveRequest leave_request;
  fillMetadata(&leave_request);
  leave_request.set_serial_id(serial_id);
  request.impose<kLeaveRequest>(leave_request);
  updateHeartbeatTime();
  if (!Hub::instance().try_request(leader_id, &request, &response)) {
    return 0;
  }
  if (response.isType<kRaftChunkRequestResponse>()) {
    proto::RaftChunkRequestResponse leave_response;
    response.extract<kRaftChunkRequestResponse>(&leave_response);
    return (leave_response.entry_index());
  }
  return false;
}

void RaftNode::sendLeaveSuccessNotification(const PeerId& peer) {
  Message request, response;
  proto::ChunkRequestMetadata metadata;
  metadata.set_table(table_name_);
  chunk_id_.serialize(metadata.mutable_chunk_id());
  request.impose<kLeaveNotification>(metadata);
  if (!Hub::instance().try_request(peer, &request, &response)) {
    LOG(WARNING) << "Leave success notification for chunk " << chunk_id_
                 << " failed for peer " << peer
                 << ". The peer is probably offline";
  }
}

proto::RaftChunkRequestResponse RaftNode::processChunkLockRequest(
    const PeerId& sender, uint64_t serial_id, bool is_retry_attempt) {
  VLOG(3) << PeerId::self() << " Received chunk lock request from " << sender
          << " for chunk " << chunk_id_;
  proto::RaftChunkRequestResponse response;
  if (!checkReadyToHandleChunkRequests()) {
    response.set_entry_index(0);
    return response;
  }
  uint64_t index = 0;
  std::lock_guard<std::mutex> chunk_lock(chunk_lock_mutex_);
  if (!raft_chunk_lock_.isLocked()) {
    std::shared_ptr<proto::RaftLogEntry> entry(new proto::RaftLogEntry);
    entry->set_lock_peer(sender.ipPort());
    entry->set_sender(sender.ipPort());
    entry->set_sender_serial_id(serial_id);
    index = leaderAppendLogEntry(entry);
  } else if (raft_chunk_lock_.isLockHolder(sender)) {
    index = raft_chunk_lock_.lock_entry_index();
  }
  response.set_entry_index(index);
  return response;
}

proto::RaftChunkRequestResponse RaftNode::processChunkUnlockRequest(
    const PeerId& sender, uint64_t serial_id, bool is_retry_attempt,
    uint64_t lock_index, uint64_t proceed_commits) {
  proto::RaftChunkRequestResponse response;
  if (!checkReadyToHandleChunkRequests()) {
    response.set_entry_index(0);
    return response;
  }
  if (!raft_chunk_lock_.isLockHolder(sender)) {
    LOG(ERROR) << "Received unlock request from a non lock holder " << sender;
    response.set_entry_index(0);
    return response;
  }
  std::shared_ptr<proto::RaftLogEntry> entry(new proto::RaftLogEntry);
  entry->set_unlock_peer(sender.ipPort());
  entry->set_unlock_proceed_commits(proceed_commits);
  entry->set_unlock_lock_index(lock_index);
  entry->set_sender(sender.ipPort());
  entry->set_sender_serial_id(serial_id);
  uint64_t index = leaderAppendLogEntry(entry);
  response.set_entry_index(index);
  return response;
}

proto::RaftChunkRequestResponse RaftNode::processChunkCommitInfo(
    const PeerId& sender, uint64_t serial_id,
    proto::ChunkCommitInfo* unowned_info_ptr) {
  proto::RaftChunkRequestResponse response;
  if (!checkReadyToHandleChunkRequests()) {
    response.set_entry_index(0);
    return response;
  }
  if (!raft_chunk_lock_.isLockHolder(sender)) {
    // TODO(aqurai): Change it to FATAL after enforcing it on sender side.
    LOG(ERROR) << "Chunk commit info received from a non lock holder peer "
               << sender << " for chunk " << chunk_id_ << ". lock holder is "
               << raft_chunk_lock_.holder();
    response.set_entry_index(0);
    return response;
  }
  std::shared_ptr<proto::RaftLogEntry> entry(new proto::RaftLogEntry);
  entry->set_allocated_multi_chunk_commit_info(
      unowned_info_ptr->release_multi_chunk_info());
  entry->set_sender(sender.ipPort());
  entry->set_sender_serial_id(serial_id);
  entry->set_multi_chunk_transaction_num_entries(
      unowned_info_ptr->num_entries());
  uint64_t index = leaderAppendLogEntry(entry);
  response.set_entry_index(index);
  return response;
}

proto::RaftChunkRequestResponse RaftNode::processInsertRequest(
    const PeerId& sender, uint64_t serial_id, bool is_retry_attempt,
    proto::Revision* unowned_revision_pointer) {
  proto::RaftChunkRequestResponse response;
  if (!checkReadyToHandleChunkRequests()) {
    response.set_entry_index(0);
    return response;
  }
  if (!raft_chunk_lock_.isLockHolder(sender)) {
    // TODO(aqurai): Change it to FATAL after enforcing it on sender side.
    LOG(ERROR) << "Insert request from a non lock holder peer " << sender
               << " for chunk " << chunk_id_ << ". lock holder is "
               << raft_chunk_lock_.holder();
    response.set_entry_index(0);
    return response;
  }
  std::shared_ptr<proto::RaftLogEntry> entry(new proto::RaftLogEntry);
  entry->set_allocated_insert_revision(unowned_revision_pointer);
  entry->set_sender(sender.ipPort());
  entry->set_sender_serial_id(serial_id);
  uint64_t index = leaderAppendLogEntry(entry);
  response.set_entry_index(index);
  return response;
}

}  // namespace map_api
