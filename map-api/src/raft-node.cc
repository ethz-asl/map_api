#include "map-api/raft-node.h"

#include <algorithm>
#include <future>
#include <random>

#include <multiagent-mapping-common/conversions.h>

#include "./chunk.pb.h"
#include "./raft.pb.h"
#include "map-api/hub.h"
#include "map-api/logical-time.h"
#include "map-api/message.h"
#include "map-api/revision.h"
#include "multiagent-mapping-common/reader-writer-lock.h"

namespace map_api {

// TODO(aqurai): decide good values for these
constexpr int kHeartbeatTimeoutMs = 150;
constexpr int kHeartbeatSendPeriodMs = 50;
constexpr int kJoinResponseTimeoutMs = 1000;
// Maximum number of yet-to-be-committed entries allowed in the log.
constexpr int kMaxLogQueueLength = 50;

// TODO(aqurai): Defined new message strings for raft chunk. Some will have to
// removed once the raft chunk implementation is complete.
const char RaftNode::kAppendEntries[] = "raft_node_append_entries";
const char RaftNode::kAppendEntriesResponse[] = "raft_node_append_response";
const char RaftNode::kInsertRequest[] = "raft_node_insert_request";
const char RaftNode::kUpdateRequest[] = "raft_node_update_request";
const char RaftNode::kInsertResponse[] = "raft_node_insert_response";
const char RaftNode::kVoteRequest[] = "raft_node_vote_request";
const char RaftNode::kVoteResponse[] = "raft_node_vote_response";
const char RaftNode::kJoinQuitRequest[] = "raft_node_join_quit_request";
const char RaftNode::kJoinQuitResponse[] = "raft_node_join_quit_response";
const char RaftNode::kConnectRequest[] = "raft_node_connect_request";
const char RaftNode::kConnectResponse[] = "raft_node_connect_response";
const char RaftNode::kInitRequest[] = "raft_node_init_response";
const char RaftNode::kQueryState[] = "raft_node_query_state";
const char RaftNode::kQueryStateResponse[] = "raft_node_query_state_response";
const char RaftNode::kNotifyJoinQuitSuccess[] = "raft_node_notify_join_success";

MAP_API_PROTO_MESSAGE(RaftNode::kAppendEntries, proto::AppendEntriesRequest);
MAP_API_PROTO_MESSAGE(RaftNode::kAppendEntriesResponse,
                      proto::AppendEntriesResponse);
MAP_API_PROTO_MESSAGE(RaftNode::kInsertRequest, proto::InsertRequest);
MAP_API_PROTO_MESSAGE(RaftNode::kUpdateRequest, proto::InsertRequest);
MAP_API_PROTO_MESSAGE(RaftNode::kInsertResponse, proto::InsertResponse);
MAP_API_PROTO_MESSAGE(RaftNode::kVoteRequest, proto::VoteRequest);
MAP_API_PROTO_MESSAGE(RaftNode::kVoteResponse, proto::VoteResponse);
MAP_API_PROTO_MESSAGE(RaftNode::kJoinQuitRequest, proto::JoinQuitRequest);
MAP_API_PROTO_MESSAGE(RaftNode::kJoinQuitResponse, proto::JoinQuitResponse);
MAP_API_PROTO_MESSAGE(RaftNode::kNotifyJoinQuitSuccess, proto::NotifyJoinQuitSuccess);
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
      num_peers_(0),
      is_join_notified_(false),
      join_log_index_(0),
      last_vote_request_term_(0),
      commit_index_(0),
      committed_result_(0) {
  election_timeout_ms_ = setElectionTimeout();
  VLOG(1) << "Peer " << PeerId::self()
          << ": Election timeout = " << election_timeout_ms_;
  data_ = new RaftChunkDataRamContainer;
  std::shared_ptr<proto::RaftLogEntry> default_entry(
      new proto::RaftLogEntry);
  default_entry->set_index(0);
  default_entry->set_entry(0);
  default_entry->set_term(0);
  LogWriteAccess log_writer(data_);
  log_writer->push_back(default_entry);
}

void RaftNode::start() {
  if (state_thread_running_) {
    LOG(FATAL) << "Start failed. State manager thread is already running.";
    return;
  }
  if (state_ == State::JOINING && !join_request_peer_.isValid() &&
      peer_list_.empty()) {
    LOG(WARNING) << "No peer information for sending join request. Exiting.";
    return;
  }
  if (!is_exiting_) {
    state_manager_thread_ = std::thread(&RaftNode::stateManagerThread, this);
  }
}

void RaftNode::stop() {
  if (state_thread_running_ && !is_exiting_) {
    is_exiting_ = true;
    giveUpLeadership();
    state_manager_thread_.join();
  }
}

uint64_t RaftNode::term() const {
  std::lock_guard<std::mutex> lock(state_mutex_);
  return current_term_;
}

const PeerId& RaftNode::leader() const {
  std::lock_guard<std::mutex> lock(state_mutex_);
  return leader_id_;
}

RaftNode::State RaftNode::state() const {
  std::lock_guard<std::mutex> lock(state_mutex_);
  return state_;
}

inline void RaftNode::setAppendEntriesResponse(
    proto::AppendEntriesResponse* response, proto::AppendResponseStatus status,
    uint64_t current_commit_index, uint64_t current_term,
    uint64_t last_log_index, uint64_t last_log_term) const {
  response->set_response(status);
  response->set_commit_index(current_commit_index);
  response->set_term(current_term);
  response->set_last_log_index(last_log_index);
  response->set_last_log_term(last_log_term);
}

void RaftNode::handleConnectRequest(const PeerId& sender, Message* response) {
  proto::ConnectResponse connect_response;
  uint64_t current_term;
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
      current_term = current_term_;
      std::shared_ptr<proto::RaftLogEntry> new_entry(new proto::RaftLogEntry);
      new_entry->set_add_peer(sender.ipPort());
      entry_index =
          leaderAppendLogEntryLocked(log_writer, new_entry, current_term_);
      connect_response.set_index(entry_index);
    }
  }

  if (entry_index > 0) {
    // Wait till it is committed, or else return 0.
    while (true) {
      std::unique_lock<std::mutex> state_lock(state_mutex_);
      if (commit_index() >= entry_index) {
        connect_response.set_index(entry_index);
        VLOG(1) << "Leader accepted connect request from " << sender;
        break;
      } else if (state_ != State::LEADER || current_term_ != current_term) {
        connect_response.set_index(0);
        break;
      }
      state_lock.unlock();
      usleep(kHeartbeatSendPeriodMs * kMillisecondsToMicroseconds);
    }
  }
  response->impose<kConnectResponse>(connect_response);
}

// If there are no new entries, Leader sends empty message (heartbeat)
// Message contains leader commit index, used to update own commit index
// In Follower state, ONLY this thread writes to log_entries_ (via the function
//  followerAppendNewEntries(..))
void RaftNode::handleAppendRequest(proto::AppendEntriesRequest* append_request,
                                   const PeerId& sender, Message* response) {
  proto::AppendEntriesResponse append_response;
  VLOG(3) << "Received AppendRequest/Heartbeat from " << sender;

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
          commit_index(), current_term_, log_writer->lastLogIndex(),
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
    followerCommitNewEntries(log_writer, append_request, state_);
  }

  // ==========================================
  // Check if Joining peer can become follower.
  // ==========================================
  if (state_ == State::JOINING && is_join_notified_ &&
      log_writer->lastLogIndex() >= join_log_index_) {
    VLOG(1) << PeerId::self() << " has joined the raft network.";
    state_ = State::FOLLOWER;
    is_join_notified_ = false;
    join_log_index_ = 0;
  }

  setAppendEntriesResponse(&append_response, response_status, commit_index(),
                           current_term_, log_writer->lastLogIndex(),
                           log_writer->lastLogTerm());
  state_lock.unlock();
  response->impose<kAppendEntriesResponse>(append_response);
}

void RaftNode::handleInsertRequest(proto::InsertRequest* request,
                                   const PeerId& sender, Message* response) {
  std::shared_ptr<proto::RaftLogEntry> entry(new proto::RaftLogEntry);
  entry->set_allocated_insert_revision(request->release_revision());
  uint64_t index = leaderSafelyAppendLogEntry(entry);
  proto::InsertResponse insert_response;
  insert_response.set_index(index);
  response->impose<kInsertResponse>(insert_response);
}

void RaftNode::handleUpdateRequest(proto::InsertRequest* request,
                                   const PeerId& sender, Message* response) {
  std::shared_ptr<proto::RaftLogEntry> entry(new proto::RaftLogEntry);
  entry->set_allocated_update_revision(request->release_revision());
  uint64_t index = leaderSafelyAppendLogEntry(entry);
  proto::InsertResponse insert_response;
  insert_response.set_index(index);
  response->impose<kInsertResponse>(insert_response);
}


void RaftNode::handleRequestVote(const proto::VoteRequest& vote_request,
                                 const PeerId& sender, Message* response) {
  updateHeartbeatTime();
  proto::VoteResponse vote_response;
  std::lock_guard<std::mutex> state_lock(state_mutex_);
  bool is_candidate_log_newer;
  // TODO(aqurai): No need of scope?
  {
    LogReadAccess log_read(data_);
    vote_response.set_previous_log_index(log_read->lastLogIndex());
    vote_response.set_previous_log_term(log_read->lastLogTerm());

    is_candidate_log_newer =
        vote_request.last_log_term() > log_read->lastLogTerm() ||
        (vote_request.last_log_term() == log_read->lastLogTerm() &&
         vote_request.last_log_index() >= log_read->lastLogIndex());
  }
  last_vote_request_term_ =
    std::max(static_cast<uint64_t>(last_vote_request_term_), vote_request.term());
  if (state_ == State::JOINING) {
    join_request_peer_ = sender;
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
            << " in term " << current_term_;
  } else {
    VLOG(1) << "Peer " << PeerId::self().ipPort() << " is declining vote for "
            << sender << " in term " << vote_request.term() << ". Reason: "
            << (vote_request.term() > current_term_ ? ""
                                                    : "Term is equal or less. ")
            << (is_candidate_log_newer ? "" : "Log is older. ");
    vote_response.set_vote(proto::VoteResponseType::DECLINED);
  }

  response->impose<kVoteResponse>(vote_response);
  election_timeout_ms_ = setElectionTimeout();
}

void RaftNode::handleJoinQuitRequest(
    const proto::JoinQuitRequest& join_quit_request, const PeerId& sender,
    Message* response) {
  proto::JoinQuitResponse join_quit_response;
  std::unique_lock<std::mutex> state_lock(state_mutex_);
  if (state_ != State::LEADER) {
    join_quit_response.set_response(false);
    if (leader_id_.isValid()) {
      join_quit_response.set_leader_id(leader_id_.ipPort());
    }
  } else {
    LogWriteAccess log_writer(data_);
    std::lock_guard<std::mutex> tracker_lock(follower_tracker_mutex_);
    std::shared_ptr<proto::RaftLogEntry> new_entry(new proto::RaftLogEntry);
    if (join_quit_request.type() == proto::PeerRequestType::ADD_PEER) {
      if (follower_tracker_map_.count(sender) == 1) {
        // Re-joining after disconnect.
        TrackerMap::iterator it = follower_tracker_map_.find(sender);
        it->second->status = PeerStatus::JOINING;
      }
      new_entry->set_add_peer(sender.ipPort());
    } else if (join_quit_request.type() == proto::PeerRequestType::REMOVE_PEER) {
      new_entry->set_remove_peer(sender.ipPort());
    }
    uint64_t entry_index =
        leaderAppendLogEntryLocked(log_writer, new_entry, current_term_);
    if (entry_index > 0) {
      join_quit_response.set_response(true);
      if (join_quit_request.type() == proto::PeerRequestType::ADD_PEER) {
        join_quit_response.set_index(entry_index);
      }
    } else {
      join_quit_response.set_response(false);
    }
  }

  response->impose<kJoinQuitResponse>(join_quit_response);
}

void RaftNode::handleNotifyJoinQuitSuccess(
    const proto::NotifyJoinQuitSuccess& request, Message* response) {
  std::lock_guard<std::mutex> state_lock(state_mutex_);
  if (state_ == State::JOINING) {
    VLOG(1) << "Join success notification received.";
    is_join_notified_ = true;
  }
  updateHeartbeatTime();
  response->ack();
}

void RaftNode::handleQueryState(const proto::QueryState& request,
                                Message* response) {
  proto::QueryStateResponse state_response;
  std::lock_guard<std::mutex> state_lock(state_mutex_);
  state_response.set_leader_id_(leader_id_.ipPort());

  LogReadAccess log_reader(data_);
  state_response.set_last_log_index(log_reader->lastLogIndex());
  state_response.set_last_log_term(log_reader->lastLogTerm());

  std::lock_guard<std::mutex> commit_lock(commit_mutex_);
  state_response.set_commit_index(commit_index_);
  state_response.set_commit_result(committed_result_);
  response->impose<kQueryStateResponse>(state_response);
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

uint64_t RaftNode::sendInsertRequest(const Revision::ConstPtr& item) {
  if (state() == State::LEADER) {
    std::shared_ptr<proto::RaftLogEntry> entry(new proto::RaftLogEntry);
    entry->set_allocated_insert_revision(item->copyToProtoPtr());
    leaderSafelyAppendLogEntry(entry);
  } else if (state() == State::FOLLOWER) {
    Message request, response;
    proto::InsertRequest insert_request;
    proto::InsertResponse insert_response;
    fillMetadata(&insert_request);
    insert_request.set_allocated_revision(item->copyToProtoPtr());
    request.impose<kInsertRequest>(insert_request);
    if (!Hub::instance().try_request(leader(), &request, &response)) {
      VLOG(1) << "Insert request failed.";
      return 0;
    }
    response.extract<kInsertResponse>(&insert_response);
    return insert_response.index();
  }
  // Failure. Can't add new revision right now because the leader is currently
  // unknown or there is an election underway.
  return 0;
}

uint64_t RaftNode::sendUpdateRequest(const Revision::ConstPtr& item) {
  if (state() == State::LEADER) {
    std::shared_ptr<proto::RaftLogEntry> entry(new proto::RaftLogEntry);
    entry->set_allocated_update_revision(item->copyToProtoPtr());
    leaderSafelyAppendLogEntry(entry);
  } else if (state() == State::FOLLOWER) {
    Message request, response;
    proto::InsertRequest insert_request;
    proto::InsertResponse insert_response;
    fillMetadata(&insert_request);
    insert_request.set_allocated_revision(item->copyToProtoPtr());
    request.impose<kUpdateRequest>(insert_request);
    if (!Hub::instance().try_request(leader(), &request, &response)) {
      VLOG(1) << "Update request failed.";
      return 0;
    }
    response.extract<kInsertResponse>(&insert_response);
    return insert_response.index();
  }
  // Failure. Can't add new revision right now because the leader is currently
  // unknown or there is an election underway.
  return 0;
}

RaftNode::VoteResponse RaftNode::sendRequestVote(const PeerId& peer,
                                                 uint64_t term,
                                                 uint64_t last_log_index,
                                                 uint64_t last_log_term) const {
  Message request, response;
  proto::VoteRequest vote_request;
  vote_request.set_term(term);
  vote_request.set_commit_index(commit_index());
  vote_request.set_last_log_index(last_log_index);
  vote_request.set_last_log_term(last_log_term);
  fillMetadata(&vote_request);
  request.impose<kVoteRequest>(vote_request);
  if (Hub::instance().try_request(peer, &request, &response)) {
    proto::VoteResponse vote_response;
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

proto::JoinQuitResponse RaftNode::sendJoinQuitRequest(
    const PeerId& peer, proto::PeerRequestType type) const {
  Message request, response;
  proto::JoinQuitRequest join_quit_request;
  join_quit_request.set_type(type);
  fillMetadata(&join_quit_request);
  request.impose<kJoinQuitRequest>(join_quit_request);
  proto::JoinQuitResponse join_response;
  updateHeartbeatTime();
  if (Hub::instance().try_request(peer, &request, &response)) {
    response.extract<kJoinQuitResponse>(&join_response);
    return join_response;
  }
  join_response.set_response(false);
  return join_response;
}

void RaftNode::sendNotifyJoinQuitSuccess(const PeerId& peer) const {
  Message request, response;
  proto::NotifyJoinQuitSuccess notification;
  fillMetadata(&notification);
  request.impose<kNotifyJoinQuitSuccess>(notification);
  Hub::instance().try_request(peer, &request, &response);
}

// Peer lock acquired by the calling function, leaderAddPeer().
// Read lock for log acquired by leaderCommitReplicatedEntries().
bool RaftNode::sendInitRequest(const PeerId& peer,
                               const LogReadAccess& log_reader) {
  Message request, response;
  proto::InitRequest init_request;
  fillMetadata(&init_request);
  for (const PeerId& chunk_peer : peer_list_) {
    init_request.add_peer_address(chunk_peer.ipPort());
  }
  init_request.add_peer_address(PeerId::self().ipPort());

  for (ConstLogIterator it = log_reader->cbegin(); it != log_reader->cend();
       ++it) {
    // TODO(aqurai): Use proto::NewPeerInit, use set_allocated() and release().
    init_request.add_serialized_items((*it)->SerializeAsString());
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
    // Conduct election if timeout has occurred.
    if (election_timeout) {
      election_timeout = false;
      conductElection();
    }

    // Read state information.
    {
      std::lock_guard<std::mutex> state_lock(state_mutex_);
      state = state_;
      current_term = current_term_;
    }

    if (state == State::JOINING) {
      if (getTimeSinceHeartbeatMs() > kJoinResponseTimeoutMs) {
        VLOG(1) << "Joining peer: " << PeerId::self()
                << " : Heartbeat timed out. Sending Join request. ";
        joinRaft();
      }
      usleep(kJoinResponseTimeoutMs * kMillisecondsToMicroseconds);
    } else if (state == State::FOLLOWER) {
      if (getTimeSinceHeartbeatMs() > election_timeout_ms_) {
        VLOG(1) << "Follower: " << PeerId::self() << " : Heartbeat timed out. ";
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
          usleep(kHeartbeatSendPeriodMs * kMillisecondsToMicroseconds);
        }
      }
      VLOG(1) << "Peer " << PeerId::self() << " Lost leadership. ";
      tracker_lock.lock();
      leaderShutDownAllTrackes();
      VLOG(1) << "Peer " << PeerId::self() << ": Follower trackers closed. ";
    }
  }  // while(!is_exiting_)
  VLOG(1) << PeerId::self() << ": Closing the State manager thread";
  state_thread_running_ = false;
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
  for (TrackerMap::value_type& tracker : follower_tracker_map_) {
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
    for (TrackerMap::value_type& tracker : follower_tracker_map_) {
      if (tracker.second->status == PeerStatus::OFFLINE) {
        ++num_not_responding;
      }
      if (tracker.second->status == PeerStatus::OFFLINE ||
          tracker.second->status == PeerStatus::ANNOUNCED_DISCONNECTING) {
        VLOG(1) << tracker.first
                << " is offline. Shutting down the follower tracker.";
        // TODO(aqurai): NOTE: Segfault here, sometimes!
        // std::__shared_ptr<>::operator->().
        PeerId remove_peer = tracker.first;
        leaderShutDownTracker(remove_peer);
        std::shared_ptr<proto::RaftLogEntry> new_entry(new proto::RaftLogEntry);
        new_entry->set_remove_peer(remove_peer.ipPort());
        leaderAppendLogEntryLocked(log_writer, new_entry, current_term);
        peer_list_.erase(remove_peer);
        num_peers_ = peer_list_.size();
      }
    }
    tracker_lock.unlock();
    peer_lock.unlock();
  }

  // num_peers_ > 1 condition is needed to prevent the leader from thinking it
  // itself is disconnected when the last peer leaves (announced or sudden).
  // TODO(aqurai): What if there is one peer and leader disconnects?
  if (num_peers_ > 1 && num_not_responding > num_peers_ / 2) {
    VLOG(1) << PeerId::self()
            << ": Disconnected from network. Shutting down follower trackers. ";
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    state_ = State::JOINING;
    follower_trackers_run_ = false;
    return;
  }
}

void RaftNode::leaderAddPeer(const PeerId& peer,
                             const LogReadAccess& log_reader,
                             uint64_t current_term) {
  std::lock_guard<std::mutex> peer_lock(peer_mutex_);
  std::lock_guard<std::mutex> tracker_lock(follower_tracker_mutex_);

  if (peer != PeerId::self()) {  // Add new peer.
    sendInitRequest(peer, log_reader);
    peer_list_.insert(peer);
    num_peers_ = peer_list_.size();
    leaderLaunchTracker(peer, current_term);
    VLOG(1) << "Leader has added peer " << peer
            << ". Raft group size: " << num_peers_ + 1;
  }
}

void RaftNode::leaderRemovePeer(const PeerId& peer) {
  std::lock_guard<std::mutex> peer_lock(peer_mutex_);
  std::lock_guard<std::mutex> tracker_lock(follower_tracker_mutex_);

  peer_list_.erase(peer);
  num_peers_ = peer_list_.size();
  leaderShutDownTracker(peer);
  VLOG(1) << "Leader has removed peer " << peer
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

void RaftNode::joinRaft() {
  PeerId peer;
  if (join_request_peer_.isValid()) {
    peer = join_request_peer_;
    std::lock_guard<std::mutex> peer_lock(peer_mutex_);
    peer_list_.insert(join_request_peer_);
    num_peers_ = peer_list_.size();
    join_request_peer_ = PeerId();
  } else if (num_peers_ > 0) {
    std::lock_guard<std::mutex> peer_lock(peer_mutex_);
    // TODO(aqurai): Choose a random peer?
    CHECK(!peer_list_.empty());
    std::set<PeerId>::iterator it = peer_list_.begin();
    peer = *it;
    CHECK(peer.isValid());
  } else {
    VLOG(1) << PeerId::self() << ": Unable to join RaftChunk. Exiting.";
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    state_ = State::DISCONNECTING;
    return;
  }
  VLOG(1) << PeerId::self() << ": Sending join request to " << peer;
  proto::JoinQuitResponse join_response =
      sendJoinQuitRequest(peer, proto::PeerRequestType::ADD_PEER);
  if (!join_response.response()) {
    if (join_response.has_leader_id()) {
      peer = PeerId(join_response.leader_id());
      join_response =
          sendJoinQuitRequest(peer, proto::PeerRequestType::ADD_PEER);
    }
  }

  if (join_response.response()) {
    join_log_index_ = join_response.index();
  }
}

void RaftNode::conductElection() {
  uint num_votes = 0;
  uint num_failed = 0;
  uint num_ineligible = 0;
  std::unique_lock<std::mutex> state_lock(state_mutex_);
  state_ = State::CANDIDATE;
  current_term_ = std::max(current_term_ + 1, last_vote_request_term_ + 1);
  uint64_t term = current_term_;
  leader_id_ = PeerId();
  LogReadAccess log_reader(data_);
  const uint64_t last_log_index = log_reader->lastLogIndex();
  const uint64_t last_log_term = log_reader->lastLogTerm();
  log_reader.unlockAndDisable();
  state_lock.unlock();

  VLOG(1) << "Peer " << PeerId::self() << " is an election candidate for term "
          << term;

  std::vector<std::future<VoteResponse>> responses;

  {
    std::lock_guard<std::mutex> peer_lock(peer_mutex_);
    for (const PeerId& peer : peer_list_) {
      std::future<VoteResponse> vote_response =
          std::async(std::launch::async, &RaftNode::sendRequestVote, this, peer,
                     term, last_log_index, last_log_term);
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
    state_ = State::JOINING;
    return;
  } else if (state_ == State::CANDIDATE &&
             num_votes + 1 >
                 (num_peers_ + 1 - num_failed - num_ineligible) / 2) {
    // This peer wins the election.
    state_ = State::LEADER;
    leader_id_ = PeerId::self();
    // Renew election timeout every session.
    election_timeout_ms_ = setElectionTimeout();
    VLOG(1) << "*** Peer " << PeerId::self()
            << " Elected as the leader for term " << current_term_ << " with "
            << num_votes + 1 << " votes. ***";
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
    log_writer->push_back(entry);
  }
}

void RaftNode::followerTrackerThread(const PeerId& peer, uint64_t term,
                                     FollowerTracker* const this_tracker) {
  uint64_t follower_next_index = commit_index() + 1;  // This is at least 1.
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
        LogReadAccess log_reader(data_);
        append_entries.set_commit_index(commit_index());
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
          append_entries.set_allocated_log_entry(it->get());
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
        append_entries.release_log_entry();
        continue;
      }
      this_tracker->status = PeerStatus::AVAILABLE;
      // The call to release is necessary to prevent prevent the allocated
      // memory being deleted during append_entries.Clear().
      append_entries.release_log_entry();

      follower_commit_index = append_response.commit_index();
      if (append_response.response() == proto::AppendResponseStatus::SUCCESS ||
           append_response.response() ==
               proto::AppendResponseStatus::ALREADY_PRESENT) {
        if (sending_entries) {
          // The response is from an append entry RPC, not a regular heartbeat.
          this_tracker->replication_index.store(follower_next_index);
          ++follower_next_index;
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
    log_writer->push_back(new_log_entry);
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
      // TODO(aqurai): Compare revisions here?
      if ((*(it + 1))->entry() == request->log_entry().entry() &&
          (*(it + 1))->term() == request->log_entry().term()) {
        return proto::AppendResponseStatus::ALREADY_PRESENT;
      } else {
        VLOG(1) << "Leader is erasing entries in log of " << PeerId::self()
                << ". from " << (*(it + 1))->index();
        CHECK_LT(commit_index(), (*(it + 1))->index());
        log_writer->resize(std::distance(log_writer->begin(), it + 1));
        std::shared_ptr<proto::RaftLogEntry> new_log_entry(
            request->release_log_entry());
        CHECK_EQ(new_log_entry->index(), log_writer->lastLogIndex() + 1);
        log_writer->push_back(new_log_entry);
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

void RaftNode::followerCommitNewEntries(
    const LogWriteAccess& log_writer,
    const proto::AppendEntriesRequest* request, State state) {
  CHECK_LE(commit_index(), log_writer->lastLogIndex());
  if (commit_index() < request->commit_index() &&
      commit_index() < log_writer->lastLogIndex()) {
    std::lock_guard<std::mutex> commit_lock(commit_mutex_);
    LogIterator old_commit = log_writer->getLogIteratorByIndex(commit_index_);
    commit_index_ =
        std::min(log_writer->lastLogIndex(), request->commit_index());
    uint64_t result_increment = 0;

    LogIterator new_commit = log_writer->getLogIteratorByIndex(commit_index_);
    std::for_each(old_commit + 1, new_commit + 1,
                  [&](const std::shared_ptr<proto::RaftLogEntry>& e) {
      if (e->has_entry()) {
        result_increment += e->entry();
      }
      // Joining peers don't act on add/remove peer entries.
      // TODO(aqurai): This might change with chunk join process.
      if (state == State::FOLLOWER && e->has_add_peer()) {
        followerAddPeer(PeerId(e->add_peer()));
      }
      if (state == State::FOLLOWER && e->has_remove_peer()) {
        followerRemovePeer(PeerId(e->remove_peer()));
      }
    });

    committed_result_ += result_increment;
    // TODO(aqurai): remove log later. Or increase the verbosity arg.
    VLOG_EVERY_N(1, 50) << PeerId::self() << ": Entry " << commit_index_
                        << " committed *****";
  }
}

const uint64_t& RaftNode::commit_index() const {
  std::lock_guard<std::mutex> lock(commit_mutex_);
  return commit_index_;
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
  if (log_writer->lastLogIndex() - commit_index() > kMaxLogQueueLength) {
    return 0;
  }
  PeerId invalid_id;
  return leaderAppendLogEntryLocked(log_writer, entry, current_term);
}

uint64_t RaftNode::leaderSafelyAppendLogEntry(
    const std::shared_ptr<proto::RaftLogEntry>& entry) {
  uint64_t current_term = term();
  uint64_t index = leaderAppendLogEntry(entry);
  if (index == 0) {
    return 0;
  }

  // TODO(aqurai): Although the state or term is guaranteed to change in case of
  // leader failure thus breaking the loop, still add a time out for safety?
  while (commit_index() < index) {
    std::unique_lock<std::mutex> state_lock(state_mutex_);
    if (state_ != State::LEADER || current_term_ != current_term) {
      return 0;
    }
    state_lock.unlock();
    usleep(kHeartbeatSendPeriodMs * kMillisecondsToMicroseconds);
  }
  return index;
}

uint64_t RaftNode::leaderAppendLogEntryLocked(
    const LogWriteAccess& log_writer,
    const std::shared_ptr<proto::RaftLogEntry>& new_entry,
    uint64_t current_term) {
  new_entry->set_index(log_writer->lastLogIndex() + 1);
  new_entry->set_term(current_term);
  log_writer->push_back(new_entry);
  new_entries_signal_.notify_all();
  VLOG_EVERY_N(1, 10) << "Adding entry to log with index "
                      << new_entry->index();
  return new_entry->index();
}

void RaftNode::leaderCommitReplicatedEntries(uint64_t current_term) {
  LogReadAccess log_reader(data_);
  std::lock_guard<std::mutex> commit_lock(commit_mutex_);

  uint replication_count = 0;
  {
    std::lock_guard<std::mutex> tracker_lock(follower_tracker_mutex_);
    for (TrackerMap::value_type& tracker : follower_tracker_map_) {
      if (tracker.second->replication_index.load() > commit_index_) {
        ++replication_count;
      }
    }
  }

  if (replication_count > num_peers_) {
    LOG(FATAL) << "Replication count (" << replication_count
               << ") is higher than peer size (" << num_peers_
               << ") at peer " << PeerId::self() << " for entry index "
               << commit_index_;
  }

  ConstLogIterator it =
      log_reader->getConstLogIteratorByIndex(commit_index_ + 1);
  if (it == log_reader->cend()) {
    return;
  }
  CHECK_LE(commit_index_ + 1, log_reader->lastLogIndex());

  // Commit entries from older leaders only if they are replicated on all peers,
  // because otherwise they can potentially be overwritten by new leaders.
  // (see §5.4.2 of http://ramcloud.stanford.edu/raft.pdf)
  bool ready_to_commit =
      ((*it)->term() < current_term && replication_count == num_peers_) ||
      ((*it)->term() == current_term && replication_count > num_peers_ / 2) ||
      num_peers_ == 0;

  if (ready_to_commit) {
    ++commit_index_;
    // TODO(aqurai): Remove later
    VLOG_EVERY_N(1, 10) << PeerId::self() << ": Commit index increased to "
                                          << commit_index_
                                          << " With replication count "
                                          << replication_count << " and term "
                                          << (*it)->term();
    if ((*it)->has_entry()) {
      committed_result_ += (*it)->entry();
    }
    if ((*it)->has_add_peer()) {
      leaderAddPeer(PeerId((*it)->add_peer()), log_reader, current_term);
    }
    if ((*it)->has_remove_peer()) {
      leaderRemovePeer(PeerId((*it)->remove_peer()));
    }
//    // TODO(aqurai): Send notification to quitting peers after zerommq crash
//    // issue is resolved.
      // There is a crash if one attempts to send message to a peer after a
      // previous message to the same peer timed out. So if a peer is removed
      // because it was not responding, sending a notification will cause a
      // crash. Otherwise, here I have to make a distinction between announced
      // and unannounced peer removal. Also, There could be other
      // threads/methods (eg: vote request) attempting to send message to the
      // non responsive peer, which causes a problem.
//    if ((*it)->add_remove_peer().request_type() ==
//        proto::PeerRequestType::ADD_PEER) {
//      sendNotifyJoinQuitSuccess(PeerId((*it)->add_remove_peer().peer_id()));
//    }
  }
}

bool RaftNode::giveUpLeadership() {
  std::unique_lock<std::mutex> lock(state_mutex_);
  if (state_ == State::LEADER) {
    follower_trackers_run_ = false;
    state_ = State::FOLLOWER;
    lock.unlock();

    updateHeartbeatTime();
    election_timeout_ms_ = 4 * setElectionTimeout();
    return true;
  } else {
    lock.unlock();
    return false;
  }
}

}  // namespace map_api
