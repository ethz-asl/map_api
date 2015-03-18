#include "map-api/raft.h"

#include <future>
#include <random>

#include <multiagent-mapping-common/conversions.h>

#include "./raft.pb.h"
#include "map-api/hub.h"
#include "map-api/logical-time.h"

// TODO(aqurai): decide good values for these
constexpr double kHeartbeatTimeoutMs = 100;
constexpr double kHearbeatSendPeriod = 25;

const int log_info = 0;

namespace map_api {

const char RaftCluster::kHeartbeat[] = "raft_cluster_heart_beat";
const char RaftCluster::kVoteRequest[] = "raft_cluster_vote_request";
const char RaftCluster::kVoteResponse[] = "raft_cluster_vote_response";

MAP_API_PROTO_MESSAGE(RaftCluster::kHeartbeat, proto::RaftHeartbeat);
MAP_API_PROTO_MESSAGE(RaftCluster::kVoteRequest, proto::RequestVote);
MAP_API_PROTO_MESSAGE(RaftCluster::kVoteResponse, proto::ResponseVote);

RaftCluster::RaftCluster()
    : leader_id_(PeerId("0.0.0.0:0")),
      state_(State::FOLLOWER),
      current_term_(0),
      leader_known_(false),
      last_heartbeat_(std::chrono::system_clock::now()),
      last_heartbeat_sender_(PeerId("0.0.0.0:0")),
      last_heartbeat_sender_term_(0),
      heartbeat_thread_running_(false),
      is_exiting_(false),
      final_result_(std::make_pair(0, 0)) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(150, 300);
  election_timeout_ = dist(gen);  // Renew every session??
  LOG(INFO) << "Peer " << PeerId::self()
            << ": Timeout value = " << election_timeout_;
}

RaftCluster::~RaftCluster() {
  if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
  is_exiting_ = true;
}

RaftCluster& RaftCluster::instance() {
  static RaftCluster instance;
  return instance;
}

void RaftCluster::registerHandlers() {
  Hub::instance().registerHandler(kHeartbeat, staticHandleHearbeat);
  Hub::instance().registerHandler(kVoteRequest, staticHandleRequestVote);
}

void RaftCluster::start() {
  heartbeat_thread_ = std::thread(heartbeatThread, this);
}

void RaftCluster::staticHandleHearbeat(const Message& request,
                                       Message* response) {
  instance().handleHearbeat(request, response);
}

void RaftCluster::staticHandleRequestVote(const Message& request,
                                          Message* response) {
  instance().handleRequestVote(request, response);
}

void RaftCluster::handleHearbeat(const Message& request, Message* response) {
  proto::RaftHeartbeat heartbeat;
  request.extract<kHeartbeat>(&heartbeat);

  if (log_info) LOG(INFO) << "Received heartbeat from " << request.sender();

  PeerId hb_sender = PeerId(request.sender());
  uint64_t hb_term = heartbeat.term();

  // Lock and read the state
  std::unique_lock<std::mutex> state_lck(state_mutex_);
  PeerId leader_id = leader_id_;
  State state = state_;  // only accessed in thread
  uint64_t current_term = current_term_;
  bool leader_known = leader_known_;
  state_lck.unlock();

  // See if the heartbeat sender has changed. If so, update the leader_id.
  bool sender_changed =
      (!leader_known || hb_sender != leader_id || hb_term != current_term);

  if (sender_changed) {
    if (hb_term > current_term || (hb_term == current_term && !leader_known)) {
      // Found a new leader
      state_lck.lock();
      if (state == State::LEADER) state_ = State::FOLLOWER;
      current_term_ = hb_term;
      leader_id_ = hb_sender;
      leader_known_ = true;
      state_lck.unlock();

      // Update the last heartbeat info
      last_heartbeat_mutex_.lock();
      last_heartbeat_ = std::chrono::system_clock::now();
      last_heartbeat_sender_ = hb_sender;
      last_heartbeat_sender_term_ = hb_term;
      last_heartbeat_mutex_.unlock();
    } else if (state == State::FOLLOWER && hb_term == current_term &&
               hb_sender != leader_id && current_term > 0 && leader_known) {
      // This should not happen.
      LOG(INFO) << "Peer " << PeerId::self().ipPort()
                << " has found 2 leaders in the same term (" << current_term
                << "). They are " << leader_id.ipPort() << " (current) and "
                << hb_sender.ipPort() << " (new) ";
    } else {
      // Heartbeat from a server with older term.
      // Ignore if follower.
      if (state == State::LEADER) {
        // raft->sendHeartbeat(last_hb_sender, current_term);
      }
    }
  } else {
    // No changes. Simply update last heartbeat time.
    last_heartbeat_mutex_.lock();
    last_heartbeat_ = std::chrono::system_clock::now();
    last_heartbeat_sender_ = hb_sender;
    last_heartbeat_sender_term_ = hb_term;
    last_heartbeat_mutex_.unlock();
  }

  response->ack();
}

void RaftCluster::handleRequestVote(const Message& request, Message* response) {
  proto::RequestVote req;
  proto::ResponseVote resp;
  request.extract<kVoteRequest>(&req);

  {
    std::lock_guard<std::mutex> state_lck(state_mutex_);
    if (req.term() > current_term_) {
      resp.set_vote(true);
      current_term_ = req.term();
      leader_known_ = false;
      state_ = State::FOLLOWER;
      LOG(INFO) << "Peer " << PeerId::self().ipPort() << " is voting for "
                << request.sender() << " in term " << current_term_;
    } else {
      LOG(INFO) << "Peer " << PeerId::self().ipPort()
                << " is declining vote for " << request.sender() << " in term "
                << req.term();
      resp.set_vote(false);
    }
  }
  response->impose<kVoteResponse>(resp);
  {
    std::lock_guard<std::mutex> lck(last_heartbeat_mutex_);
    last_heartbeat_ = std::chrono::system_clock::now();
  }
}

bool RaftCluster::sendHeartbeat(PeerId id, uint64_t term) {
  Message request, response;
  proto::RaftHeartbeat heartbeat;
  heartbeat.set_term(term);
  request.impose<kHeartbeat>(heartbeat);
  if (Hub::instance().try_request(id, &request, &response)) {
    if (log_info) LOG(INFO) << "Hearbeat sent to peer " << id.ipPort();
    if (response.isOk()) {
      if (log_info) LOG(INFO) << "--- Hearbeat ack'd by peer " << id.ipPort();
      return true;
    } else {
      return false;
    }
  } else {
    LOG(INFO) << "Hearbeat failed for peer " << id.ipPort();
    return false;
  }
}

int RaftCluster::sendRequestVote(PeerId id, uint64_t term) {
  Message request, response;
  proto::RequestVote vote_request;
  vote_request.set_term(term);
  vote_request.set_logical_time(LogicalTime::sample().serialize());
  request.impose<kVoteRequest>(vote_request);
  if (Hub::instance().try_request(id, &request, &response)) {
    proto::ResponseVote vote_response;
    response.extract<kVoteResponse>(&vote_response);
    if (vote_response.vote()) return 1;
    // vote granted
    else
      return 0;  // vote declined
  } else {
    return -1;  // Failed to send or no response
  }
}

void RaftCluster::heartbeatThread(RaftCluster* raft) {
  TimePoint last_correct_hb;
  bool election_timeout = false;
  raft->heartbeat_thread_running_ = true;

  while (!raft->is_exiting_) {
    // See if new heartbeat from new leader is received.
    // TimePoint last_hb_time;
    PeerId last_hb_sender;

    // Lock and read the last heartbeat info
    {
      std::lock_guard<std::mutex> lck(raft->last_heartbeat_mutex_);
      last_correct_hb = raft->last_heartbeat_;
      last_hb_sender = raft->last_heartbeat_sender_;
    }

    // Lock and read the state
    std::unique_lock<std::mutex> state_lck(raft->state_mutex_);
    State state = raft->state_;  // only accessed in thread
    state_lck.unlock();

    // Handle the heartbeat timings. Send heartbeat periodically if in
    // Leader state. Check for heartbeat timeouts if in follower state.
    if (state == State::FOLLOWER) {
      TimePoint now = std::chrono::system_clock::now();
      double duration = static_cast<double>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              now - last_correct_hb).count());
      if (duration >= raft->election_timeout_) {
        LOG(INFO) << "Follower: " << PeerId::self()
                  << " : Hearbeat timed out !!!";
        election_timeout = true;
      } else {
        if (log_info)
          LOG(INFO) << "Follower: " << PeerId::self() << " : Hearbeat received";
        usleep(5000);
        continue;
      }
    }

    // Handle election
    if (election_timeout) {
      election_timeout = false;
      state_lck.lock();
      raft->state_ = State::CANDIDATE;
      uint64_t term = ++raft->current_term_;
      raft->leader_known_ = false;
      state_lck.unlock();

      LOG(INFO) << "Peer " << PeerId::self()
                << " is an election candidate for term " << term;

      raft->follower_handler_wait_ = true;
      raft->election_won_ = false;
      raft->follower_handler_run_ = false;
      for (auto peer : raft->peer_list_) {
        raft->follower_handlers_.emplace_back(&RaftCluster::followerHandler,
                                              raft, peer, term);
      }

      while (raft->num_vote_responses_ != raft->peer_list_.size()) {
        usleep(100);
      }

      // See if there are enough votes, and if someone else became leader,
      // making this follower
      state_lck.lock();
      if (raft->state_ == State::CANDIDATE &&
          raft->num_votes_ >= raft->peer_list_.size() / 2) {
        raft->state_ = State::LEADER;
        raft->leader_known_ = true;
        raft->leader_id_ = PeerId::self();
        raft->election_won_ = true;
        LOG(INFO) << "Peer " << PeerId::self()
                  << "Elected as the leader for term " << raft->current_term_;
      } else {
        raft->election_won_ = false;
        raft->state_ = State::FOLLOWER;
        raft->leader_known_ = false;
      }
      LOG(INFO) << "Peer " << PeerId::self() << " Received " << raft->num_votes_
                << " votes in term " << term;
      state_lck.unlock();

      // Set the election timeout again
      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_int_distribution<> dist(150, 300);
      raft->election_timeout_ = dist(gen);  // Renew every session??

      if (raft->election_won_) {
        state = State::LEADER;
        raft->follower_handler_run_ = true;
        raft->follower_handler_wait_ = false;
        while (state == State::LEADER) {
          usleep(5000);
          state_lck.lock();
          state = raft->state_;
          state_lck.unlock();
        }
        raft->follower_handler_run_ = false;
        for (std::thread& follower_thread : raft->follower_handlers_) {
          if (follower_thread.joinable()) follower_thread.join();
        }
        raft->follower_handlers_.clear();

      } else {
        raft->follower_handler_run_ = false;
        raft->follower_handler_wait_ = false;
      }
    }
  }  // while(!raft->is_exiting_)
  raft->heartbeat_thread_running_ = false;
}

void RaftCluster::followerHandler(PeerId peer, uint64_t term) {
  int result = sendRequestVote(peer, term);
  switch (result) {
    case 1:
      ++num_vote_responses_;
      ++num_votes_;
      break;
    case 0:
      ++num_vote_responses_;
      break;
    case -1:
      // TODO(aqurai): handle non-responding peer
      break;
  }

  while (follower_handler_wait_) {
    usleep(1000);
  }

  if (!election_won_) {
    return;
  }

  TimePoint last_heartbeat = std::chrono::system_clock::now();
  sendHeartbeat(peer, term);

  while (follower_handler_run_) {
    TimePoint now = std::chrono::system_clock::now();
    double duration = static_cast<double>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now - last_heartbeat).count());
    if (duration > kHearbeatSendPeriod) {
      last_heartbeat = now;
      // send heartbeat
      sendHeartbeat(peer, term);
    }
    usleep(5000);
    continue;
  }
}

void RaftCluster::setElectionTimeout() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(150, 300);
  // raft->election_timeout_ = dist(gen); // Renew every session??
}

}  // namespace map_api
