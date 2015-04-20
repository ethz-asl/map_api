#include "map-api/raft-node.h"

#include <algorithm>
#include <future>
#include <random>

#include <multiagent-mapping-common/conversions.h>

#include "./raft.pb.h"
#include "map-api/hub.h"
#include "map-api/logical-time.h"
#include "map-api/message.h"
#include "map-api/reader-writer-lock.h"

namespace map_api {

// TODO(aqurai): decide good values for these
constexpr int kHeartbeatTimeoutMs = 150;
constexpr int kHeartbeatSendPeriodMs = 50;
constexpr int kJoinResponseTimeoutMs = 1000;
constexpr int kMaxLogQueueLength = 20;

const char RaftNode::kAppendEntries[] = "raft_node_append_entries";
const char RaftNode::kAppendEntriesResponse[] = "raft_node_append_response";
const char RaftNode::kVoteRequest[] = "raft_node_vote_request";
const char RaftNode::kVoteResponse[] = "raft_node_vote_response";
const char RaftNode::kAddRemovePeer[] = "raft_node_add_remove_peer";
const char RaftNode::kJoinQuitRequest[] = "raft_node_join_quit_request";
const char RaftNode::kJoinQuitResponse[] = "raft_node_join_quit_response";
const char RaftNode::kNotifyJoinQuitSuccess[] = "raft_node_notify_join_success";
const char RaftNode::kQueryState[] = "raft_node_query_state";
const char RaftNode::kQueryStateResponse[] = "raft_node_query_state_response";

MAP_API_PROTO_MESSAGE(RaftNode::kAppendEntries, proto::AppendEntriesRequest);
MAP_API_PROTO_MESSAGE(RaftNode::kAppendEntriesResponse,
                      proto::AppendEntriesResponse);
MAP_API_PROTO_MESSAGE(RaftNode::kVoteRequest, proto::VoteRequest);
MAP_API_PROTO_MESSAGE(RaftNode::kVoteResponse, proto::VoteResponse);
MAP_API_PROTO_MESSAGE(RaftNode::kJoinQuitRequest, proto::JoinQuitRequest);
MAP_API_PROTO_MESSAGE(RaftNode::kJoinQuitResponse, proto::JoinQuitResponse);
MAP_API_PROTO_MESSAGE(RaftNode::kQueryStateResponse, proto::QueryStateResponse);

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
  std::shared_ptr<proto::RaftRevision> default_revision(
      new proto::RaftRevision);
  default_revision->set_index(0);
  default_revision->set_entry(0);
  default_revision->set_term(0);
  log_entries_.push_back(default_revision);
}

RaftNode& RaftNode::instance() {
  static RaftNode instance;
  return instance;
}

void RaftNode::kill() {
  VLOG(1) << PeerId::self() << ": Closing raft instance.";
  follower_trackers_run_ = false;
  is_exiting_ = true;
  state_manager_thread_.join();
  VLOG(1) << PeerId::self() << ": Raft instance closed.";
}

void RaftNode::registerHandlers() {
  Hub::instance().registerHandler(kAppendEntries, staticHandleAppendRequest);
  Hub::instance().registerHandler(kVoteRequest, staticHandleRequestVote);
  Hub::instance().registerHandler(kQueryState, staticHandleQueryState);
  Hub::instance().registerHandler(kJoinQuitRequest, staticHandleJoinQuitRequest);
  Hub::instance().registerHandler(kNotifyJoinQuitSuccess,
                                  staticHandleNotifyJoinQuitSuccess);
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
  state_manager_thread_ = std::thread(&RaftNode::stateManagerThread, this);
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

void RaftNode::staticHandleAppendRequest(const Message& request,
                                     Message* response) {
  instance().handleAppendRequest(request, response);
}

void RaftNode::staticHandleRequestVote(const Message& request,
                                       Message* response) {
  instance().handleRequestVote(request, response);
}

void RaftNode::staticHandleQueryState(const Message& request,
                                      Message* response) {
  instance().handleQueryState(request, response);
}

void RaftNode::staticHandleJoinQuitRequest(const Message& request,
                                           Message* response) {
  instance().handleJoinQuitRequest(request, response);
}

void RaftNode::staticHandleNotifyJoinQuitSuccess(const Message& request,
                                                 Message* response) {
  instance().handleNotifyJoinQuitSuccess(request, response);
}

inline void RaftNode::setAppendEntriesResponse(
    proto::AppendResponseStatus status,
    proto::AppendEntriesResponse* response) {
  response->set_term(current_term_);
  response->set_response(status);
  response->set_last_log_index(log_entries_.back()->index());
  response->set_last_log_term(log_entries_.back()->term());
  response->set_commit_index(commit_index());
}

// If there are no new entries, Leader sends empty message (heartbeat)
// Message contains leader commit index, used to update own commit index
// In Follower state, ONLY this thread writes to log_entries_ (via the function
//  followerAppendNewEntries(..))
void RaftNode::handleAppendRequest(const Message& request, Message* response) {
  proto::AppendEntriesRequest append_request;
  proto::AppendEntriesResponse append_response;
  request.extract<kAppendEntries>(&append_request);
  VLOG(3) << "Received AppendRequest/Heartbeat from " << request.sender();

  const PeerId request_sender = PeerId(request.sender());
  const uint64_t request_term = append_request.term();

  // Lock and read the state.
  std::unique_lock<std::mutex> state_lock(state_mutex_);
  bool sender_changed =
      (request_sender != leader_id_ || request_term != current_term_ ||
       state_ == State::JOINING);

  // Lock and read log info
  log_mutex_.acquireReadLock();
  const uint64_t last_log_index = log_entries_.back()->index();
  const uint64_t last_log_term = log_entries_.back()->term();
  bool is_sender_log_newer = append_request.last_log_term() > last_log_term ||
                             (append_request.last_log_term() == last_log_term &&
                              append_request.last_log_index() >= last_log_index);

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
      leader_id_ = request_sender;
      if (state_ == State::LEADER || state_ == State::CANDIDATE) {
        state_ = State::FOLLOWER;
        follower_trackers_run_ = false;
      }
    } else if (state_ == State::FOLLOWER && request_term == current_term_ &&
               request_sender != leader_id_ && current_term_ > 0 &&
               leader_id_.isValid()) {
      // This should not happen.
      LOG(FATAL) << "Peer " << PeerId::self().ipPort()
                 << " has found 2 leaders in the same term (" << current_term_
                 << "). They are " << leader_id_.ipPort() << " (current) and "
                 << request_sender.ipPort() << " (new) ";
    } else {
      setAppendEntriesResponse(proto::AppendResponseStatus::REJECTED,
                               &append_response);
      log_mutex_.releaseReadLock();
      response->impose<kAppendEntriesResponse>(append_response);
      return;
    }
  }
  updateHeartbeatTime();

  // ==============================
  // Append/commit new log entries.
  // ==============================
  CHECK(log_mutex_.upgradeToWriteLock());

  // Check if there are new entries.
  proto::AppendResponseStatus response_status =
      followerAppendNewEntries(append_request);

  // Check if new entries are committed.
  if (response_status == proto::AppendResponseStatus::SUCCESS) {
    followerCommitNewEntries(append_request, state_);
  }

  // ==========================================
  // Check if Joining peer can become follower.
  // ==========================================
  if (state_ == State::JOINING && is_join_notified_ &&
      join_log_index_ >= log_entries_.back()->index()) {
    VLOG(1) << PeerId::self() << " has joined the raft network.";
    state_ = State::FOLLOWER;
    is_join_notified_ = false;
    join_log_index_ = 0;
  }

  setAppendEntriesResponse(response_status, &append_response);
  log_mutex_.releaseWriteLock();
  state_lock.unlock();
  response->impose<kAppendEntriesResponse>(append_response);
}

void RaftNode::handleRequestVote(const Message& request, Message* response) {
  updateHeartbeatTime();
  proto::VoteRequest vote_request;
  proto::VoteResponse vote_response;
  request.extract<kVoteRequest>(&vote_request);
  std::lock_guard<std::mutex> state_lock(state_mutex_);
  log_mutex_.acquireReadLock();
  vote_response.set_previous_log_index(log_entries_.back()->index());
  vote_response.set_previous_log_term(log_entries_.back()->term());

  bool is_candidate_log_newer =
      vote_request.last_log_term() > log_entries_.back()->term() ||
      (vote_request.last_log_term() == log_entries_.back()->term() &&
       vote_request.last_log_index() >= log_entries_.back()->index());
  log_mutex_.releaseReadLock();
  last_vote_request_term_ =
    std::max(static_cast<uint64_t>(last_vote_request_term_), vote_request.term());
  if (state_ == State::JOINING) {
    join_request_peer_ = request.sender();
    vote_response.set_vote(proto::VoteResponseType::VOTER_NOT_ELIGIBLE);
  } else if (vote_request.term() > current_term_ && is_candidate_log_newer) {
    vote_response.set_vote(proto::VoteResponseType::GRANTED);
    current_term_ = vote_request.term();
    leader_id_ = PeerId();
    if (state_ == State::LEADER) {
      follower_trackers_run_ = false;
    }
    state_ = State::FOLLOWER;
    VLOG(1) << "Peer " << PeerId::self().ipPort() << " is voting for "
            << request.sender() << " in term " << current_term_;
  } else {
    VLOG(1) << "Peer " << PeerId::self().ipPort() << " is declining vote for "
            << request.sender() << " in term " << vote_request.term()
            << ". Reason: " << (vote_request.term() > current_term_
                                    ? ""
                                    : "Term is equal or less. ")
            << (is_candidate_log_newer ? "" : "Log is older. ");
    vote_response.set_vote(proto::VoteResponseType::DECLINED);
  }

  response->impose<kVoteResponse>(vote_response);
  election_timeout_ms_ = setElectionTimeout();
}

void RaftNode::handleJoinQuitRequest(const Message& request, Message* response) {
  proto::JoinQuitRequest join_quit_request;
  proto::JoinQuitResponse join_quit_response;
  request.extract<kJoinQuitRequest>(&join_quit_request);
  std::unique_lock<std::mutex> state_lock(state_mutex_);
  if (state_ != State::LEADER) {
    join_quit_response.set_response(false);
    if (leader_id_.isValid()) {
      join_quit_response.set_leader_id(leader_id_.ipPort());
    }
  } else {
    ScopedWriteLock log_lock(&log_mutex_);
    std::lock_guard<std::mutex> tracker_lock(follower_tracker_mutex_);
    if (follower_tracker_map_.count(request.sender()) == 1) {
      // Re-joining after disconnect.
      TrackerMap::iterator it = follower_tracker_map_.find(request.sender());
      it->second->status = PeerStatus::JOINING;
    }
    uint64_t entry_index = leaderAddEntryToLog(
        0, current_term_, request.sender(), join_quit_request.type());
    if (entry_index > 0) {
      join_quit_response.set_response(true);
      if (join_quit_request.type() == proto::PeerRequestType::ADD_PEER) {
        join_quit_response.set_index(entry_index);
        std::lock_guard<std::mutex> peer_lock(peer_mutex_);
        for (const PeerId& peer : peer_list_) {
          join_quit_response.add_peer_id(peer.ipPort());
        }
      }
    } else {
      join_quit_response.set_response(false);
    }
  }
  response->impose<kJoinQuitResponse>(join_quit_response);
}

void RaftNode::handleNotifyJoinQuitSuccess(const Message& request,
                                           Message* response) {
  std::lock_guard<std::mutex> state_lock(state_mutex_);
  if (state_ == State::JOINING) {
    is_join_notified_ = true;
  }
  updateHeartbeatTime();
  response->ack();
}

void RaftNode::handleQueryState(const Message& request, Message* response) {
  proto::QueryStateResponse state_response;
  std::lock_guard<std::mutex> state_lock(state_mutex_);
  state_response.set_leader_id_(leader_id_.ipPort());

  ScopedReadLock log_lock(&log_mutex_);
  state_response.set_last_log_index(log_entries_.back()->index());
  state_response.set_last_log_term(log_entries_.back()->term());

  std::lock_guard<std::mutex> commit_lock(commit_mutex_);
  state_response.set_commit_index(commit_index_);
  state_response.set_commit_result(committed_result_);
  response->impose<kQueryStateResponse>(state_response);
}

bool RaftNode::sendAppendEntries(
    const PeerId& peer, const proto::AppendEntriesRequest& append_entries,
    proto::AppendEntriesResponse* append_response) {
  Message request, response;
  request.impose<kAppendEntries>(append_entries);
  if (Hub::instance().try_request(peer, &request, &response)) {
    response.extract<kAppendEntriesResponse>(append_response);
    return true;
  } else {
    VLOG(1) << "AppendEntries RPC failed for peer " << peer.ipPort();
    return false;
  }
}

RaftNode::VoteResponse RaftNode::sendRequestVote(const PeerId& peer, uint64_t term,
                              uint64_t last_log_index, uint64_t last_log_term) {
  Message request, response;
  proto::VoteRequest vote_request;
  vote_request.set_term(term);
  vote_request.set_commit_index(commit_index());
  vote_request.set_last_log_index(last_log_index);
  vote_request.set_last_log_term(last_log_term);
  request.impose<kVoteRequest>(vote_request);
  if (Hub::instance().try_request(peer, &request, &response)) {
    proto::VoteResponse vote_response;
    response.extract<kVoteResponse>(&vote_response);
    if (vote_response.vote() == proto::VoteResponseType::GRANTED)
      return VoteResponse::VOTE_GRANTED;
    else if (vote_response.vote() ==
             proto::VoteResponseType::VOTER_NOT_ELIGIBLE)
      return VoteResponse::VOTER_NOT_ELIGIBLE;
    else
      return VoteResponse::VOTE_DECLINED;
  } else {
    return VoteResponse::FAILED_REQUEST;
  }
}

proto::JoinQuitResponse RaftNode::sendJoinQuitRequest(
    const PeerId& peer, proto::PeerRequestType type) {
  Message request, response;
  proto::JoinQuitRequest join_quit_request;
  join_quit_request.set_type(type);
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

void RaftNode::sendNotifyJoinQuitSuccess(const PeerId& peer) {
  Message request, response;
  request.impose<kNotifyJoinQuitSuccess>();
  Hub::instance().try_request(peer, &request, &response);
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
                << " : Heartbeat timed out. Sending Join request again.";
        joinRaft();
      } else {
        usleep(kJoinResponseTimeoutMs * kMillisecondsToMicroseconds);
      }
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
                                        peer, current_term, tracker);
}

void RaftNode::leaderMonitorFollowerStatus(uint64_t current_term) {
  uint num_not_responding = 0;
  log_mutex_.acquireWriteLock();
  std::unique_lock<std::mutex> peer_lock(peer_mutex_);
  std::unique_lock<std::mutex> tracker_lock(follower_tracker_mutex_);
  for (TrackerMap::value_type& tracker : follower_tracker_map_) {
    if (tracker.second->status == PeerStatus::OFFLINE) {
      ++num_not_responding;
    }
    if (tracker.second->status == PeerStatus::OFFLINE ||
        tracker.second->status == PeerStatus::ANNOUNCED_DISCONNECTING) {
      VLOG(1) << tracker.first << " is offline. Shutting down the follower tracker.";
      // TODO(aqurai): NOTE: Segfault here, sometimes! std::__shared_ptr<>::operator->().
      PeerId remove_peer = tracker.first;
      leaderShutDownTracker(remove_peer);
      leaderAddEntryToLog(0, current_term, remove_peer,
                          proto::PeerRequestType::REMOVE_PEER);
      peer_list_.erase(remove_peer);
      num_peers_ = peer_list_.size();
    }
  }
  tracker_lock.unlock();
  peer_lock.unlock();
  log_mutex_.releaseWriteLock();

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

void RaftNode::leaderAddRemovePeer(const PeerId& peer,
                                   proto::PeerRequestType request,
                                   uint64_t current_term) {
  std::lock_guard<std::mutex> peer_lock(peer_mutex_);
  std::lock_guard<std::mutex> tracker_lock(follower_tracker_mutex_);
  if (request == proto::PeerRequestType::REMOVE_PEER) {
    peer_list_.erase(peer);
    num_peers_ = peer_list_.size();
    leaderShutDownTracker(peer);
    VLOG(1) << "Leader has removed peer " << peer;
  } else if (peer != PeerId::self()) {  // Add new peer.
    peer_list_.insert(peer);
    num_peers_ = peer_list_.size();
    leaderLaunchTracker(peer, current_term);
    VLOG(1) << "Leader has added peer " << peer;
  }
}

void RaftNode::followerAddRemovePeer(const proto::AddRemovePeer& add_remove_peer) {
  std::lock_guard<std::mutex> peer_lock(peer_mutex_);
  if (add_remove_peer.request_type() == proto::PeerRequestType::ADD_PEER) {
    const PeerId& peer = PeerId(add_remove_peer.peer_id());
    if (peer != PeerId::self()) {
      peer_list_.insert(peer);
      num_peers_ = peer_list_.size();
    }
  } else {
    peer_list_.erase(PeerId(add_remove_peer.peer_id()));
    num_peers_ = peer_list_.size();
  }
}

void RaftNode::joinRaft() {
  PeerId peer;
  if (join_request_peer_.isValid()) {
    peer = join_request_peer_;
    join_request_peer_ = PeerId();
  } else if (num_peers_ > 0) {
    // TODO(aqurai): lock peers here?
    // TODO(aqurai): Choose a random peer?
    std::set<PeerId>::iterator it = peer_list_.begin();
    peer = *it;
    CHECK(peer.isValid());
  } else {
    VLOG(1) << PeerId::self() << ": Unable to join raft. Exiting.";
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
    peer_list_.clear();
    peer_list_.insert(peer);
    uint num_response_peers = join_response.peer_id_size();
    std::lock_guard<std::mutex> peer_lock(peer_mutex_);
    for (uint i = 0; i < num_response_peers; ++i) {
      peer_list_.insert(PeerId(join_response.peer_id(i)));
    }
    num_peers_ = peer_list_.size();
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
  log_mutex_.acquireReadLock();
  const uint64_t last_log_index = log_entries_.back()->index();
  const uint64_t last_log_term = log_entries_.back()->term();
  log_mutex_.releaseReadLock();
  state_lock.unlock();

  VLOG(1) << "Peer " << PeerId::self() << " is an election candidate for term "
          << term;

  std::vector<std::future<VoteResponse>> responses;

  std::unique_lock<std::mutex> peer_lock(peer_mutex_);
  for (const PeerId& peer : peer_list_) {
    std::future<VoteResponse> vote_response =
        std::async(std::launch::async, &RaftNode::sendRequestVote, this, peer,
                   term, last_log_index, last_log_term);
    responses.push_back(std::move(vote_response));
  }
  peer_lock.unlock();

  for (std::future<VoteResponse>& response : responses) {
    VoteResponse vote_response = response.get();
    if (vote_response == VoteResponse::VOTE_GRANTED)
      ++num_votes;
    else if (vote_response == VoteResponse::VOTE_GRANTED)
      ++num_ineligible;
    else if (vote_response == VoteResponse::FAILED_REQUEST)
      ++num_failed;
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
    LOG(WARNING) << "Here 4";
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

void RaftNode::followerTrackerThread(
    const PeerId& peer, uint64_t term,
    const std::shared_ptr<FollowerTracker> this_tracker) {
  uint64_t follower_next_index = commit_index() + 1;  // This is at least 1.
  uint64_t follower_commit_index = 0;
  proto::AppendEntriesRequest append_entries;
  proto::AppendEntriesResponse append_response;

  // This lock is only used for waiting on a condition variable
  // (new_entries_signal_), which is notified when there is a new log entry.
  std::mutex wait_mutex;
  std::unique_lock<std::mutex> wait_lock(wait_mutex);

  while (follower_trackers_run_ && this_tracker->tracker_run) {
    bool append_successs = false;
    while (!append_successs && follower_trackers_run_ &&
           this_tracker->tracker_run) {
      if (this_tracker->status == PeerStatus::OFFLINE) {
        VLOG(1) << "Peer is offline. Not calling sendAppendEntries.";
        usleep(kJoinResponseTimeoutMs);
        continue;
      }
      bool sending_entries = false;
      append_entries.Clear();
      append_response.Clear();
      append_entries.set_term(term);
      log_mutex_.acquireReadLock();
      append_entries.set_commit_index(commit_index());
      uint64_t last_log_index = log_entries_.back()->index();
      append_entries.set_last_log_index(log_entries_.back()->index());
      append_entries.set_last_log_term(log_entries_.back()->term());
      if (follower_next_index <= log_entries_.back()->index()) {
        sending_entries = true;
        // There is at least one new entry to be sent.
        LogIterator it = getLogIteratorByIndex(follower_next_index);

        // if this is the case, the control shouldn't have reached here,
        CHECK(it != log_entries_.end());
        CHECK(it != log_entries_.begin());
        append_entries.set_allocated_revision(it->get());
        append_entries.set_previous_log_index((*(it - 1))->index());
        append_entries.set_previous_log_term((*(it - 1))->term());
      }
      log_mutex_.releaseReadLock();

      if (!sendAppendEntries(peer, append_entries, &append_response)) {
        if (this_tracker->status == PeerStatus::AVAILABLE) {
          this_tracker->status = PeerStatus::OFFLINE;
          VLOG(1) << PeerId::self() << ": Failed sendAppendEntries to " << peer;
        } else {
          this_tracker->status = PeerStatus::OFFLINE;
          // this_tracker->tracker_run = false;
          VLOG(1) << PeerId::self() << ": " << peer << " appears offline.";
        }
        continue;
      }
      this_tracker->status = PeerStatus::AVAILABLE;
      // The call ro release is necessary to prevent prevent the allocated
      // memory being deleted during append_entries.Clear().
      append_entries.release_revision();

      follower_commit_index = append_response.commit_index();
      if (append_response.response() == proto::AppendResponseStatus::SUCCESS ||
           append_response.response() ==
               proto::AppendResponseStatus::ALREADY_PRESENT) {
        if (sending_entries) {
          // The response is from an append entry RPC, not a regular heartbeat.
          this_tracker->replication_index.store(follower_next_index);
          ++follower_next_index;
          append_successs = (follower_next_index > last_log_index);
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

// Assumes at least read lock is acquired for log_mutex_.
RaftNode::LogIterator RaftNode::getLogIteratorByIndex(uint64_t index) {
  LogIterator it = log_entries_.end();
  if (index < log_entries_.front()->index() ||
      index > log_entries_.back()->index()) {
    return it;
  } else {
    // The log indices are always sequential.
    it = log_entries_.begin() + (index - log_entries_.front()->index());
    CHECK_EQ((*it)->index(), index);
    return it;
  }
}

proto::AppendResponseStatus RaftNode::followerAppendNewEntries(
    proto::AppendEntriesRequest& request) {
  if (!request.has_revision()) {
    // No need to proceed further as message contains no new entries
    return proto::AppendResponseStatus::SUCCESS;
  } else if (request.previous_log_index() == log_entries_.back()->index() &&
             request.previous_log_term() == log_entries_.back()->term()) {
    // All is well, append the new entry, but don't commit it yet.
    std::shared_ptr<proto::RaftRevision> new_revision(
        request.release_revision());
    CHECK_EQ(new_revision->index(), log_entries_.back()->index() + 1);
    log_entries_.push_back(new_revision);
    return proto::AppendResponseStatus::SUCCESS;
  } else if (request.previous_log_index() < log_entries_.back()->index()) {
    // Leader sends an older entry due to a conflict
    LogIterator it = getLogIteratorByIndex(request.previous_log_index());
    if (it != log_entries_.end() &&
        request.previous_log_term() == (*it)->term()) {
      // The received entry matched one of the older entries in the log.
      CHECK(it != log_entries_.end());
      CHECK((it + 1) != log_entries_.end());
      // Erase and replace only of the entry is different from the one already
      // stored.
      if ((*(it + 1))->entry() == request.revision().entry() &&
          (*(it + 1))->term() == request.revision().term()) {
        return proto::AppendResponseStatus::ALREADY_PRESENT;
      } else {
        VLOG(1) << "Leader is erasing entries in log of " << PeerId::self()
                << ". from " << (*(it + 1))->index();
        CHECK_LT(commit_index(), (*(it + 1))->index());
        log_entries_.resize(std::distance(log_entries_.begin(), it + 1));
        std::shared_ptr<proto::RaftRevision> new_revision(
            request.release_revision());
        CHECK_EQ(new_revision->index(), log_entries_.back()->index() + 1);
        log_entries_.push_back(new_revision);
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
    const proto::AppendEntriesRequest& request, State state) {
  CHECK_LE(commit_index(), log_entries_.back()->index());
  if (commit_index() < request.commit_index() &&
      commit_index() < log_entries_.back()->index()) {
    std::lock_guard<std::mutex> commit_lock(commit_mutex_);
    LogIterator old_commit = getLogIteratorByIndex(commit_index_);
    commit_index_ =
        std::min(log_entries_.back()->index(), request.commit_index());
    uint64_t result_increment = 0;

    LogIterator new_commit = getLogIteratorByIndex(commit_index_);
    std::for_each(old_commit + 1, new_commit + 1,
                  [&](const std::shared_ptr<proto::RaftRevision>& e) {
      if (e->has_entry()) {
        result_increment += e->entry();
      }
      if (state == State::FOLLOWER && e->has_add_remove_peer()) {
        followerAddRemovePeer(e->add_remove_peer());
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

const uint64_t& RaftNode::committed_result() const {
  std::lock_guard<std::mutex> lock(commit_mutex_);
  return committed_result_;
}

uint64_t RaftNode::leaderAppendLogEntry(uint32_t entry) {
  uint64_t current_term;
  {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    if (state_ != State::LEADER) {
      return 0;
    }
    current_term = current_term_;
  }
  ScopedWriteLock log_lock(&log_mutex_);
  if (log_entries_.back()->index() - commit_index() > kMaxLogQueueLength) {
    return 0;
  }
  PeerId invalid_id;
  return leaderAddEntryToLog(entry, current_term, invalid_id);
}

uint64_t RaftNode::leaderAddEntryToLog(uint32_t entry, uint32_t current_term,
                                       const PeerId& peer_id,
                                       proto::PeerRequestType request_type) {
  std::shared_ptr<proto::RaftRevision> new_revision(new proto::RaftRevision);
  new_revision->set_index(log_entries_.back()->index() + 1);
  new_revision->set_entry(entry);
  new_revision->set_term(current_term);

  if (peer_id.isValid()) {
    proto::AddRemovePeer* add_remove_peer = new proto::AddRemovePeer;
    add_remove_peer->set_peer_id(peer_id.ipPort());
    add_remove_peer->set_request_type(request_type);
    new_revision->set_allocated_add_remove_peer(add_remove_peer);
  }
  log_entries_.push_back(new_revision);
  new_entries_signal_.notify_all();
  VLOG_EVERY_N(1, 10) << "Adding entry to log with index "
                      << new_revision->index();
  return new_revision->index();
}

void RaftNode::leaderCommitReplicatedEntries(uint64_t current_term) {
  ScopedReadLock log_lock(&log_mutex_);
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

  LogIterator it = getLogIteratorByIndex(commit_index_ + 1);
  if (it == log_entries_.end()) {
    return;
  }
  CHECK_LE(commit_index_ + 1, log_entries_.back()->index());

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
    if ((*it)->has_add_remove_peer()) {
      leaderAddRemovePeer(PeerId((*it)->add_remove_peer().peer_id()),
                          (*it)->add_remove_peer().request_type(),
                          current_term);
      sendNotifyJoinQuitSuccess(PeerId((*it)->add_remove_peer().peer_id()));
    }
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
