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
      is_exiting_(false) {
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

  last_heartbeat_mutex_.lock();
  last_heartbeat_ = std::chrono::system_clock::now();
  last_heartbeat_sender_ = PeerId(request.sender());
  last_heartbeat_sender_term_ = heartbeat.term();
  last_heartbeat_mutex_.unlock();
  usleep(1000);
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
    TimePoint last_hb_time;
    PeerId last_hb_sender;
    uint64_t last_hb_sender_term;

    // Lock and read the last heartbeat info
    {
      std::lock_guard<std::mutex> lck(raft->last_heartbeat_mutex_);
      last_hb_time = raft->last_heartbeat_;
      last_hb_sender = raft->last_heartbeat_sender_;
      last_hb_sender_term = raft->last_heartbeat_sender_term_;
    }

    // Lock and read the state
    std::unique_lock<std::mutex> state_lck(raft->state_mutex_);
    PeerId leader_id = raft->leader_id_;
    State state = raft->state_;  // only accessed in thread
    uint64_t current_term = raft->current_term_;
    bool leader_known = raft->leader_known_;
    state_lck.unlock();

    // See if the heartbeat sender has changed. If so, update the leader_id.
    bool sender_changed = (!leader_known || last_hb_sender != leader_id ||
                           last_hb_sender_term != current_term);

    if (sender_changed) {
      if (last_hb_sender_term > current_term ||
          (last_hb_sender_term == current_term && !leader_known)) {
        // Found a new leader
        state_lck.lock();
        if (state == State::LEADER) raft->state_ = State::FOLLOWER;
        raft->current_term_ = last_hb_sender_term;
        raft->leader_id_ = last_hb_sender;
        raft->leader_known_ = true;
        state_lck.unlock();
        last_correct_hb = last_hb_time;
        continue;
      } else if (state == State::FOLLOWER &&
                 last_hb_sender_term == current_term &&
                 last_hb_sender != leader_id && current_term > 0 &&
                 leader_known) {
        // This should not happen.
        LOG(INFO) << "Peer " << PeerId::self().ipPort()
                  << " has found 2 leaders in the same term (" << current_term
                  << "). They are " << leader_id.ipPort() << " (current) and "
                  << last_hb_sender.ipPort() << " (new) ";
      } else {
        // Heartbeat from a server with older term.
        // Ignore if follower.
        if (state == State::LEADER) {
          // raft->sendHeartbeat(last_hb_sender, current_term);
        }
      }
    } else {
      // No changes. Simply update last heartbeat time.
      last_correct_hb = last_hb_time;
    }

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
    } else if (state == State::LEADER) {
      TimePoint now = std::chrono::system_clock::now();
      double duration = static_cast<double>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              now - last_correct_hb).count());
      if (duration > kHearbeatSendPeriod) {
        last_correct_hb = now;
        if (log_info) LOG(INFO) << "Leader: Time to send new hearbeat";
        // send heartbeat
        int num_ack = 0;
        std::vector<std::future<bool>> responses;
        for (auto peer : raft->peer_list_) {
          std::future<bool> p =
              std::async(std::launch::async, &RaftCluster::sendHeartbeat, raft,
                         peer, current_term);
          responses.push_back(std::move(p));
        }

        for (std::future<bool>& response : responses) {
          if (response.get()) ++num_ack;
        }
        if (num_ack == 0) {  // This means we are disconnected
          state_lck.lock();
          raft->state_ = State::FOLLOWER;
          raft->leader_known_ = false;
          state_lck.unlock();
        }
      }
      usleep(5000);
      continue;
    }

    // Handle state changes if in follower state and heartbeat timeout occurs.
    if (election_timeout) {
      state_lck.lock();
      raft->state_ = State::CANDIDATE;
      uint64_t term = ++raft->current_term_;
      raft->leader_known_ = false;
      state_lck.unlock();
      uint32_t num_votes = 0;

      LOG(INFO) << "Peer " << PeerId::self()
                << " is an election candidate for term " << term;
      for (auto peer : raft->peer_list_) {
        int result = raft->sendRequestVote(peer, term);
        if (result == 1)
          ++num_votes;
        else if (result == -1) {
          // TODO(aqurai): Handle non responding peer
        }
      }
      LOG(INFO) << "Peer " << PeerId::self() << " Received " << num_votes
                << " votes in term " << term;

      // See if there are enough votes, and if someone else became leader,
      // making
      // this follower
      state_lck.lock();
      if (raft->state_ == State::CANDIDATE &&
          num_votes >= raft->peer_list_.size() / 2) {
        raft->state_ = State::LEADER;
        raft->leader_known_ = true;
        raft->leader_id_ = PeerId::self();
        LOG(INFO) << "Peer " << PeerId::self()
                  << "Elected as the leader for term " << raft->current_term_;
      } else {
        raft->state_ = State::FOLLOWER;
        raft->leader_known_ = false;
      }
      state_lck.unlock();

      election_timeout = false;

      // Reset the last heartbeat time so that the next timeout is not based on
      // a stale heartbeat.
      {
        std::lock_guard<std::mutex> lck(raft->last_heartbeat_mutex_);
        raft->last_heartbeat_ = std::chrono::system_clock::now();
      }

      // Set the election timeout again
      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_int_distribution<> dist(150, 300);
      raft->election_timeout_ = dist(gen);  // Renew every session??
    }
  }  // while(!raft->is_exiting_)
  raft->heartbeat_thread_running_ = false;
}

void RaftCluster::setElectionTimeout() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(150, 300);
  // raft->election_timeout_ = dist(gen); // Renew every session??
}

}  // namespace map_api
