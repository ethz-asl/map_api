#include "map-api/raft-node.h"

#include <algorithm>
#include <future>
#include <random>
#include <sys/param.h>

#include <multiagent-mapping-common/conversions.h>

#include "./raft.pb.h"
#include "map-api/hub.h"
#include "map-api/logical-time.h"
#include "map-api/message.h"
#include "map-api/reader-writer-lock.h"

// TODO(aqurai): decide good values for these
constexpr int kHeartbeatTimeoutMs = 50;
constexpr int kHeartbeatSendPeriodMs = 25;

namespace map_api {

const char RaftNode::kAppendEntries[] = "raft_cluster_append_entries";
const char RaftNode::kAppendEntriesResponse[] = "raft_cluster_append_response";
const char RaftNode::kVoteRequest[] = "raft_cluster_vote_request";
const char RaftNode::kVoteResponse[] = "raft_cluster_vote_response";

MAP_API_PROTO_MESSAGE(RaftNode::kAppendEntries, proto::AppendEntriesRequest);
MAP_API_PROTO_MESSAGE(RaftNode::kAppendEntriesResponse,
                      proto::AppendEntriesResponse);
MAP_API_PROTO_MESSAGE(RaftNode::kVoteRequest, proto::RequestVote);
MAP_API_PROTO_MESSAGE(RaftNode::kVoteResponse, proto::ResponseVote);

const PeerId kInvalidId = PeerId();

RaftNode::RaftNode()
    : leader_id_(PeerId()),
      state_(State::FOLLOWER),
      current_term_(0),
      last_heartbeat_(std::chrono::system_clock::now()),
      heartbeat_thread_running_(false),
      is_exiting_(false),
      final_result_(std::make_pair(0, 0)),
      last_applied_index_(0),
      commit_index_(0) {
  election_timeout_ = setElectionTimeout();
  VLOG(1) << "Peer " << PeerId::self()
          << ": Election timeout = " << election_timeout_;
  LogEntry default_entry;
  default_entry.index = 0;
  default_entry.term = 0;
  default_entry.entry = 0;
  default_entry.replication_count = 0;
  log_entries_.push_back(default_entry);
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
  Hub::instance().registerHandler(kAppendEntries, staticHandleHeartbeat);
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

void RaftNode::staticHandleHeartbeat(const Message& request,
                                     Message* response) {
  instance().handleHeartbeat(request, response);
}

void RaftNode::staticHandleRequestVote(const Message& request,
                                       Message* response) {
  instance().handleRequestVote(request, response);
}

void RaftNode::handleHeartbeat(const Message& request, Message* response) {
  proto::AppendEntriesRequest append_request;
  proto::AppendEntriesResponse append_response;
  request.extract<kAppendEntries>(&append_request);

  VLOG(2) << "Received heartbeat from " << request.sender();

  PeerId hb_sender = PeerId(request.sender());
  uint64_t hb_term = append_request.term();

  // Lock and read the state.
  std::unique_lock<std::mutex> state_lock(state_mutex_);

  // See if the heartbeat sender has changed. If so, update the leader_id.
  bool sender_changed = (!leader_id_.isValid() || hb_sender != leader_id_ ||
                         hb_term != current_term_);

  if (sender_changed) {
    if (hb_term > current_term_ ||
        (hb_term == current_term_ && !leader_id_.isValid())) {
      // Update state and leader info if another leader with newer term is found
      // or, if a leader is found when a there isn't a known one.
      current_term_ = hb_term;
      leader_id_ = hb_sender;
      if (state_ == State::LEADER) {
        state_ = State::FOLLOWER;
        follower_trackers_run_ = false;
      }

      // Update the last heartbeat info.
      std::unique_lock<std::mutex> heartbeat_lock(last_heartbeat_mutex_);
      last_heartbeat_ = std::chrono::system_clock::now();
    } else if (state_ == State::FOLLOWER && hb_term == current_term_ &&
               hb_sender != leader_id_ && current_term_ > 0 &&
               leader_id_.isValid()) {
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

  append_response.set_term(current_term_);
  state_lock.unlock();

  log_mutex_.acquireReadLock();
  append_response.set_previouslogindex(log_entries_.back().index);
  append_response.set_previouslogterm(log_entries_.back().term);
  append_response.set_commitindex(commit_index_);

  if (!append_request.has_newentry() || !append_request.has_newentryterm()) {
    // No need to proceed further as message contains no new entries
    log_mutex_.releaseReadLock();
    append_response.set_success(true);
    response->impose<kAppendEntriesResponse>(append_response);
    return;
  }

  if (append_request.previouslogindex() == log_entries_.back().index &&
      append_request.previouslogterm() == log_entries_.back().term) {
    // All is well, append the new entry, but don't commit it yet.
    LogEntry new_entry;
    new_entry.index = log_entries_.back().index + 1;
    new_entry.term = append_request.newentryterm();
    new_entry.entry = append_request.newentry();
    new_entry.replication_count = 0;
    if (!log_mutex_.upgradeToWriteLock()) {
      log_mutex_.acquireWriteLock();
    }
    log_entries_.push_back(new_entry);
    append_response.set_previouslogindex(log_entries_.back().index);
    append_response.set_previouslogterm(log_entries_.back().term);
    append_response.set_success(true);
    log_mutex_.releaseWriteLock();

  } else if (append_request.previouslogindex() < log_entries_.back().index) {
    // Leader sends an older entry due to a conflict
    std::vector<LogEntry>::iterator it =
        getIteratorByIndex(append_request.previouslogindex());

    if (it != log_entries_.end() &&
        append_request.previouslogterm() == it->term) {
      // The received entry matched one of the older entries in the log.

      // TODO(aqurai) remove CHECK later??
      CHECK(it->index == append_request.previouslogindex());
      if (!log_mutex_.upgradeToWriteLock()) {
        log_mutex_.acquireWriteLock();
      }
      log_entries_.erase(it + 1, log_entries_.end() - 1);
      LogEntry new_entry;
      new_entry.index = log_entries_.back().index + 1;
      new_entry.term = append_request.newentryterm();
      new_entry.entry = append_request.newentry();
      new_entry.replication_count = 0;
      log_entries_.push_back(new_entry);
      append_response.set_previouslogindex(log_entries_.back().index);
      append_response.set_previouslogterm(log_entries_.back().term);
      append_response.set_success(true);
      log_mutex_.releaseWriteLock();
    } else {
      append_response.set_success(false);
      log_mutex_.releaseReadLock();
    }
  } else {
    // term and index don't match with leader's log.
    append_response.set_success(false);
    log_mutex_.releaseReadLock();
  }

  log_mutex_.acquireReadLock();
  if (commit_index_ < append_request.commitindex() &&
      commit_index_ < log_entries_.back().index) {
    std::vector<LogEntry>::iterator it = getIteratorByIndex(commit_index_);
    commit_index_ =
        MIN(log_entries_.back().index, append_request.commitindex());
    uint64_t result = final_result_.second;

    // TODO (aqurai) try lambda + std::for_each
    for (LogEntry& e : std::vector<LogEntry>(it + 1, it + commit_index_)) {
      result += e.term;
    }

    if (!log_mutex_.upgradeToWriteLock()) {
      log_mutex_.acquireWriteLock();
    }
    final_result_ =
        std::make_pair(static_cast<uint64_t>(commit_index_), result);
    log_mutex_.releaseWriteLock();
  } else {
    log_mutex_.releaseReadLock();
  }
  append_response.set_commitindex(commit_index_);
  response->impose<kAppendEntriesResponse>(append_response);
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
      leader_id_ = PeerId();
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
  proto::AppendEntriesRequest heartbeat;
  heartbeat.set_term(term);
  request.impose<kAppendEntries>(heartbeat);
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

bool RaftNode::sendAppendEntries(
    const PeerId& peer, proto::AppendEntriesRequest& append_entries,
    proto::AppendEntriesResponse* append_response) {
  Message request, response;
  request.impose<kAppendEntries>(append_entries);
  if (Hub::instance().try_request(peer, &request, &response)) {
    response.extract<kAppendEntriesResponse>(append_response);
    return true;
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
        usleep(election_timeout_ * 1000);
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

      std::mutex wait_mutex;
      std::unique_lock<std::mutex> wait_lock(wait_mutex);
      while (follower_trackers_run_) {
        // call commit new entries
        if (setElectionTimeout() < 90) {
          appendLogEntry(19);
          VLOG(1) << "Adding entry to log";
        }
        commitReplicatedEntries();
        entry_replicated_signal_.wait(wait_lock);
      }

      for (std::thread& follower_thread : follower_trackers_) {
        follower_thread.join();
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
  leader_id_ = PeerId();
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
    leader_id_ = PeerId::self();
  } else {
    // This peer doesn't win the election.
    state_ = State::FOLLOWER;
    leader_id_ = PeerId();
  }
  state_lock.unlock();
}

void RaftNode::followerTracker(const PeerId& peer, uint64_t term) {
  uint64_t follower_next_index = commit_index_ + 1;  // This is at least 1.
  uint64_t follower_commit_index = 0;
  std::mutex wait_mutex;
  std::unique_lock<std::mutex> wait_lock(wait_mutex);
  proto::AppendEntriesRequest append_entries;
  proto::AppendEntriesResponse append_response;

  while (follower_trackers_run_) {
    append_entries.Clear();
    append_response.Clear();
    append_entries.set_term(term);
    bool append_successs = false;
    while (!append_successs) {
      log_mutex_.acquireReadLock();
      append_entries.set_previouslogindex(log_entries_.back().index);
      append_entries.set_previouslogterm(log_entries_.back().term);
      append_entries.set_commitindex(commit_index_);

      if (follower_next_index > log_entries_.back().index) {
        // There are no new entries to send.
        append_entries.set_previouslogindex(log_entries_.back().index);
        append_entries.set_previouslogterm(log_entries_.back().term);
      } else if (follower_next_index <= log_entries_.back().index) {
        // There is at least one new entry to be sent.
        std::vector<LogEntry>::iterator it =
            getIteratorByIndex(follower_next_index);
        // if this is the case, the control shouldn't have reached here,
        CHECK(it != log_entries_.end());
        append_entries.set_newentry(it->entry);
        append_entries.set_newentryterm(it->term);
        // TODO (aqurai): verify that (it-1) never throws an error.
        append_entries.set_previouslogindex((it - 1)->index);
        append_entries.set_previouslogterm((it - 1)->term);
      }

      append_entries.set_commitindex(commit_index_);
      log_mutex_.releaseReadLock();
      sendAppendEntries(peer, append_entries, &append_response);

      follower_commit_index = append_response.commitindex();
      append_successs = append_response.success();
      if (!append_successs) {
        // Append on follower failed due to a conflict. Send an older entry
        // and try again.
        --follower_next_index;
        if (append_response.commitindex() >= follower_next_index) {
          // This should not happen.
          LOG(FATAL) << "Conflicting entry already committed on peer " << peer
                     << ". Peer commit index " << follower_commit_index
                     << ". Peer last log index, term "
                     << append_response.previouslogindex() << ", "
                     << append_response.previouslogterm() << ". "
                     << "Leader last log index, term "
                     << append_entries.previouslogindex() << ", "
                     << append_entries.previouslogterm() << ". ";
        }
      } else {
        if (follower_next_index == append_response.previouslogindex()) {
          // The response is from an append entry RPF, not a regular heartbeat.
          log_mutex_.acquireWriteLock();
          std::vector<LogEntry>::iterator it =
              getIteratorByIndex(follower_next_index);
          ++it->replication_count;
          log_mutex_.releaseWriteLock();
          ++follower_next_index;
          entry_replicated_signal_.notify_all();
        }
      }
    }  //  while (!append_successs)

    new_entries_signal_.wait_for(
        wait_lock, std::chrono::milliseconds(kHeartbeatSendPeriodMs));
  }
}

int RaftNode::setElectionTimeout() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(kHeartbeatTimeoutMs,
                                       3 * kHeartbeatTimeoutMs);
  return dist(gen);
}

// Assumes at least read lock is acquired for log_mutex_
std::vector<RaftNode::LogEntry>::iterator RaftNode::getIteratorByIndex(
    uint64_t index) {
  std::vector<LogEntry>::iterator it = log_entries_.end();
  if (index < log_entries_.front().index || index > log_entries_.back().index) {
    return it;
  } else {
    // The log indexes are always sequential.
    return log_entries_.begin() + (index - log_entries_.front().index);
  }
}

void RaftNode::appendLogEntry(uint32_t entry) {
  uint64_t current_term;
  {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    if (state_ != State::LEADER) {
      return;
    }
    current_term = current_term_;
  }
  log_mutex_.acquireWriteLock();
  LogEntry new_entry;
  if (log_entries_.empty()) {
    new_entry.index = 0;

  } else {
    new_entry.index = log_entries_.back().index + 1;
  }
  new_entry.term = current_term;
  new_entry.entry = entry;
  new_entry.replication_count = 0;
  log_entries_.push_back(new_entry);
  log_mutex_.releaseWriteLock();
  new_entries_signal_.notify_all();
}

void RaftNode::commitReplicatedEntries() {
  log_mutex_.acquireReadLock();
  std::vector<LogEntry>::iterator it = getIteratorByIndex(commit_index_ + 1);
  if (it != log_entries_.end()) {
    if (!(it->replication_count <= peer_list_.size())) {
      LOG(ERROR) << "Replication count (" << it->replication_count
                 << ") is higher than peer size (" << peer_list_.size()
                 << ") at peer " << PeerId::self();
    }
    if (it->replication_count > peer_list_.size() / 2) {
      // Replicated on more than half of the peers.
      ++commit_index_;
      VLOG(1) << "Commit index increased to " << commit_index_
              << " With replication count " << it->replication_count;
      uint64_t old_result = final_result_.second;
      if (!log_mutex_.upgradeToWriteLock()) {
        log_mutex_.acquireWriteLock();
      }
      final_result_ = std::make_pair(it->term, old_result + it->entry);
      log_mutex_.releaseWriteLock();
      return;
    }
  }
  log_mutex_.releaseReadLock();
}

}  // namespace map_api
