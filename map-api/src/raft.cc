#include "map-api/raft.h"

#include <future>
#include <random>

#include <multiagent-mapping-common/conversions.h>

#include "./raft.pb.h"
#include "map-api/hub.h"
#include "map-api/logical-time.h"
#include "map-api/message.h"

// TODO(aqurai): decide good values for these
constexpr double kHeartbeatTimeoutMs = 50;
constexpr double kHeartbeatSendPeriodMs = 25;

namespace map_api {

const char RaftNode::kHeartbeat[] = "raft_cluster_heart_beat";
const char RaftNode::kVoteRequest[] = "raft_cluster_vote_request";
const char RaftNode::kVoteResponse[] = "raft_cluster_vote_response";

MAP_API_PROTO_MESSAGE(RaftNode::kHeartbeat, proto::RaftHeartbeat);
MAP_API_PROTO_MESSAGE(RaftNode::kVoteRequest, proto::RequestVote);
MAP_API_PROTO_MESSAGE(RaftNode::kVoteResponse, proto::ResponseVote);

RaftNode::RaftNode()
    : state_(State::FOLLOWER),
      current_term_(0),
      is_leader_known_(false),
      last_heartbeat_(std::chrono::system_clock::now()),
      heartbeat_thread_running_(false),
      is_exiting_(false) {
  election_timeout_ = setElectionTimeout();
  VLOG(1) << "Peer " << PeerId::self()
          << ": Election timeout = " << election_timeout_;
}

RaftNode::~RaftNode() {
  is_exiting_ = true;
  if (heartbeat_thread_.joinable()) {
    heartbeat_thread_.join();
  }
}

RaftNode& RaftNode::instance() {
  static RaftNode instance;
  return instance;
}

void RaftNode::registerHandlers() {
  Hub::instance().registerHandler(kHeartbeat, staticHandleHeartbeat);
  Hub::instance().registerHandler(kVoteRequest, staticHandleRequestVote);
}

void RaftNode::start() {
  heartbeat_thread_ = std::thread(&RaftNode::heartbeatThread, this);
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

bool RaftNode::is_leader_known() const {
  std::lock_guard<std::mutex> lock(state_mutex_);
  return is_leader_known_;
}

void RaftNode::staticHandleHeartbeat(const Message& request,
                                     Message* response) {
  instance().handleHeartbeat(request, response);
}

void RaftNode::staticHandleRequestVote(const Message& request,
                                       Message* response) {
  instance().handleRequestVote(request, response);
}

void RaftNode::handleHeartbeat(const Message& request, Message* response) {
  proto::RaftHeartbeat heartbeat;
  request.extract<kHeartbeat>(&heartbeat);

  VLOG(2) << "Received heartbeat from " << request.sender();

  PeerId hb_sender = PeerId(request.sender());
  uint64_t hb_term = heartbeat.term();

  // Lock and read the state.
  std::unique_lock<std::mutex> state_lock(state_mutex_);

  // See if the heartbeat sender has changed. If so, update the leader_id.
  bool sender_changed = (!is_leader_known_ || hb_sender != leader_id_ ||
                         hb_term != current_term_);

  if (sender_changed) {
    if (hb_term > current_term_ ||
        (hb_term == current_term_ && !is_leader_known_)) {
      // Update state and leader info if another leader with newer term is found
      // or, if a leader is found when a there isn't a known one.
      current_term_ = hb_term;
      leader_id_ = hb_sender;
      is_leader_known_ = true;
      if (state_ == State::LEADER) {
        state_ = State::FOLLOWER;
        follower_trackers_run_ = false;
      }

      // Update the last heartbeat info.
      std::unique_lock<std::mutex> heartbeat_lock(last_heartbeat_mutex_);
      last_heartbeat_ = std::chrono::system_clock::now();
    } else if (state_ == State::FOLLOWER && hb_term == current_term_ &&
               hb_sender != leader_id_ && current_term_ > 0 &&
               is_leader_known_) {
      // This should not happen.
      LOG(FATAL) << "Peer " << PeerId::self().ipPort()
                 << " has found 2 leaders in the same term (" << current_term_
                 << "). They are " << leader_id_.ipPort() << " (current) and "
                 << hb_sender.ipPort() << " (new) ";
    } else {
      // TODO(aqurai) Handle heartbeat from a server with older term.
    }
  } else {
    // Leader didn't change. Simply update last heartbeat time.
    std::unique_lock<std::mutex> heartbeat_lock(last_heartbeat_mutex_);
    last_heartbeat_ = std::chrono::system_clock::now();
  }
  state_lock.unlock();
  response->ack();
}

void RaftNode::handleRequestVote(const Message& request, Message* response) {
  proto::RequestVote req;
  proto::ResponseVote resp;
  request.extract<kVoteRequest>(&req);
  {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    if (req.term() > current_term_) {
      resp.set_vote(true);
      current_term_ = req.term();
      is_leader_known_ = false;
      if (state_ == State::LEADER) {
        follower_trackers_run_ = false;
      }
      state_ = State::FOLLOWER;
      VLOG(1) << "Peer " << PeerId::self().ipPort() << " is voting for "
              << request.sender() << " in term " << current_term_;
    } else {
      VLOG(1) << "Peer " << PeerId::self().ipPort() << " is declining vote for "
              << request.sender() << " in term " << req.term();
      resp.set_vote(false);
    }
  }

  response->impose<kVoteResponse>(resp);
  {
    std::lock_guard<std::mutex> lock(last_heartbeat_mutex_);
    last_heartbeat_ = std::chrono::system_clock::now();
  }
}

bool RaftNode::sendHeartbeat(const PeerId& peer, uint64_t term) {
  Message request, response;
  proto::RaftHeartbeat heartbeat;
  heartbeat.set_term(term);
  request.impose<kHeartbeat>(heartbeat);
  if (Hub::instance().try_request(peer, &request, &response)) {
    if (response.isOk())
      return true;
    else
      return false;
  } else {
    VLOG(3) << "Heartbeat failed for peer " << peer.ipPort();
    return false;
  }
}

int RaftNode::sendRequestVote(const PeerId& peer, uint64_t term) {
  Message request, response;
  proto::RequestVote vote_request;
  vote_request.set_term(term);
  request.impose<kVoteRequest>(vote_request);
  if (Hub::instance().try_request(peer, &request, &response)) {
    proto::ResponseVote vote_response;
    response.extract<kVoteResponse>(&vote_response);
    if (vote_response.vote())
      return VOTE_GRANTED;
    else
      return VOTE_DECLINED;
  } else {
    return FAILED_REQUEST;
  }
}

void RaftNode::heartbeatThread() {
  TimePoint last_hb_time;
  bool election_timeout = false;
  State state;
  uint64_t current_term;
  heartbeat_thread_running_ = true;

  while (!is_exiting_) {
    // Conduct election if timeout has occurred.
    if (election_timeout) {
      election_timeout = false;
      conductElection();
      // Renew election timeout every session.
      election_timeout_ = setElectionTimeout();
    }

    // Read state information.
    {
      std::lock_guard<std::mutex> state_lock(state_mutex_);
      state = state_;
      current_term = current_term_;
    }

    if (state == State::FOLLOWER) {
      // Check for heartbeat timeout if in follower state.
      {
        std::lock_guard<std::mutex> lock(last_heartbeat_mutex_);
        last_hb_time = last_heartbeat_;
      }
      TimePoint now = std::chrono::system_clock::now();
      double duration_ms = static_cast<double>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              now - last_hb_time).count());
      if (duration_ms >= election_timeout_) {
        VLOG(1) << "Follower: " << PeerId::self() << " : Heartbeat timed out. ";
        election_timeout = true;
      } else {
        usleep(election_timeout_);
      }
    } else if (state == State::LEADER) {
      // Launch follower_handler threads if state is LEADER.
      VLOG(1) << "Peer " << PeerId::self() << " Elected as the leader for term "
              << current_term_;
      follower_trackers_run_ = true;
      for (const PeerId& peer : peer_list_) {
        follower_trackers_.emplace_back(&RaftNode::followerTracker, this, peer,
                                        current_term);
      }

      for (std::thread& follower_thread : follower_trackers_) {
        if (follower_thread.joinable()) {
          follower_thread.join();
        }
      }
      follower_trackers_.clear();
    }
  }  // while(!is_exiting_)
  heartbeat_thread_running_ = false;
}

void RaftNode::conductElection() {
  uint16_t num_votes = 0;
  std::unique_lock<std::mutex> state_lock(state_mutex_);
  state_ = State::CANDIDATE;
  uint64_t term = ++current_term_;
  is_leader_known_ = false;
  state_lock.unlock();

  VLOG(1) << "Peer " << PeerId::self() << " is an election candidate for term "
          << term;

  std::vector<std::future<int>> responses;
  for (const PeerId& peer : peer_list_) {
    std::future<int> p = std::async(
        std::launch::async, &RaftNode::sendRequestVote, this, peer, term);
    responses.push_back(std::move(p));
  }
  for (std::future<int>& response : responses) {
    if (response.get() == VOTE_GRANTED) {
      ++num_votes;
    } else {
      // TODO(aqurai) Handle non-responding peers
    }
  }

  state_lock.lock();
  if (state_ == State::CANDIDATE && num_votes >= peer_list_.size() / 2) {
    // This peer wins the election.
    state_ = State::LEADER;
    is_leader_known_ = true;
    leader_id_ = PeerId::self();
  } else {
    // This peer doesn't win the election.
    state_ = State::FOLLOWER;
    is_leader_known_ = false;
  }
  state_lock.unlock();
}

void RaftNode::followerTracker(const PeerId& peer, uint64_t term) {
  while (follower_trackers_run_) {
    sendHeartbeat(peer, term);
    usleep(kHeartbeatSendPeriodMs);
  }
}

int RaftNode::setElectionTimeout() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(kHeartbeatTimeoutMs,
                                       3 * kHeartbeatTimeoutMs);
  return dist(gen);
}

}  // namespace map_api
