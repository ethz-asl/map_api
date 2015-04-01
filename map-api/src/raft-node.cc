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
      state_thread_running_(false),
      is_exiting_(false),
      last_vote_request_term_(0),
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
  std::stringstream s;
  s << "Add log entry A: index, term, count = " << default_entry.index << ", "
    << default_entry.term << ", " << log_entries_.size();
  loglog.push_back(s.str());
}

RaftNode::~RaftNode() {
  is_exiting_ = true;
  if (state_manager_thread_.joinable()) {
    state_manager_thread_.join();
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
  state_manager_thread_ = std::thread(&RaftNode::stateManagerThread, this);
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
  instance().handleAppendRequest(request, response);
}

void RaftNode::staticHandleRequestVote(const Message& request,
                                       Message* response) {
  instance().handleRequestVote(request, response);
}

// If there are no new entries, Leader sends empty AppendRequest Message
// (heartbeat)
// Message contains leader commit index, used to update own commit index
// In Follower state, ONLY this thread writes to log_entries_
void RaftNode::handleAppendRequest(const Message& request, Message* response) {
  proto::AppendEntriesRequest append_request;
  proto::AppendEntriesResponse append_response;
  request.extract<kAppendEntries>(&append_request);

  VLOG(3) << "Received AppendRequest/Heartbeat from " << request.sender();

  PeerId hb_sender = PeerId(request.sender());
  uint64_t request_term = append_request.term();

  // Lock and read log info
  log_mutex_.acquireReadLock();
  uint64_t last_log_index = log_entries_.back().index;
  uint64_t last_log_term = log_entries_.back().term;
  log_mutex_.releaseReadLock();
  bool is_sender_log_newer =
      append_request.previouslogterm() > last_log_term ||
      (append_request.previouslogterm() == last_log_term &&
       append_request.previouslogindex() >= last_log_index);

  // Lock and read the state.
  std::unique_lock<std::mutex> state_lock(state_mutex_);
  append_response.set_term(current_term_);
  bool sender_changed = (!leader_id_.isValid() || hb_sender != leader_id_ ||
                         request_term != current_term_);

  // ===============================
  // Check if the sender has changed
  // ===============================

  if (sender_changed) {
    if (request_term > current_term_ ||
        (request_term == current_term_ && !leader_id_.isValid()) ||
        (request_term < current_term_ && !leader_id_.isValid() &&
         is_sender_log_newer)) {
      // Update state and leader info if another leader with newer term is found
      // or, if a leader is found when a there isn't a known one.
      current_term_ = request_term;
      leader_id_ = hb_sender;
      if (state_ == State::LEADER) {
        state_ = State::FOLLOWER;
        follower_trackers_run_ = false;
      }

      // Update the last heartbeat info.
      std::unique_lock<std::mutex> heartbeat_lock(last_heartbeat_mutex_);
      last_heartbeat_ = std::chrono::system_clock::now();
    } else if (state_ == State::FOLLOWER && request_term == current_term_ &&
               hb_sender != leader_id_ && current_term_ > 0 &&
               leader_id_.isValid()) {
      // This should not happen.
      LOG(FATAL) << "Peer " << PeerId::self().ipPort()
                 << " has found 2 leaders in the same term (" << current_term_
                 << "). They are " << leader_id_.ipPort() << " (current) and "
                 << hb_sender.ipPort() << " (new) ";
    } else {
      // TODO(aqurai) Handle heartbeat from a server with older term and log.
      append_response.set_success(false);
      log_mutex_.acquireReadLock();
      append_response.set_previouslogindex(log_entries_.back().index);
      append_response.set_previouslogterm(log_entries_.back().term);
      append_response.set_commitindex(commit_index_);
      log_mutex_.releaseReadLock();
      response->impose<kAppendEntriesResponse>(append_response);
      return;
    }
  } else {
    // Leader didn't change. Simply update last heartbeat time.
    std::unique_lock<std::mutex> heartbeat_lock(last_heartbeat_mutex_);
    last_heartbeat_ = std::chrono::system_clock::now();
  }
  state_lock.unlock();

  // ==============================
  // Check if there are new entries
  // ==============================
  log_mutex_.acquireReadLock();
  append_response.set_previouslogindex(log_entries_.back().index);
  append_response.set_previouslogterm(log_entries_.back().term);
  append_response.set_commitindex(commit_index_);

  if (!append_request.has_newentry() || !append_request.has_newentryterm()) {
    // No need to proceed further as message contains no new entries
    log_mutex_.releaseReadLock();
    append_response.set_success(true);

  } else if (append_request.previouslogindex() == log_entries_.back().index &&
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
      VLOG(1) << "Leader is erasing entries in log of " << PeerId::self()
              << ". from " << (it + 1)->index;
      log_entries_.erase(it + 1, log_entries_.end());
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

  // ==================================
  // Check if new entries are committed
  // ==================================
  log_mutex_.acquireReadLock();
  if (append_response.success() &&
      commit_index_ < append_request.commitindex() &&
      commit_index_ < log_entries_.back().index) {
    std::vector<LogEntry>::iterator it = getIteratorByIndex(commit_index_);
    CHECK(it->index == commit_index_);
    commit_index_ =
        MIN(log_entries_.back().index, append_request.commitindex());
    uint64_t result = final_result_.second;

    // TODO(aqurai): try lambda + std::for_each
    std::vector<LogEntry>::iterator it2 = getIteratorByIndex(commit_index_);
    for (LogEntry& e : std::vector<LogEntry>(it + 1, it2 + 1)) {
      result += e.entry;
    }

    if (!log_mutex_.upgradeToWriteLock()) {
      log_mutex_.acquireWriteLock();
    }
    final_result_ =
        std::make_pair(static_cast<uint64_t>(commit_index_), result);
    VLOG_IF(1, commit_index_ % 50 == 0)
        << PeerId::self() << ": Entry " << commit_index_
        << " committed ***************************";
    // todo before commit: remove this?
    CHECK(commit_index_ * 19 == final_result_.second);
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
  log_mutex_.acquireReadLock();
  uint64_t last_log_index = log_entries_.back().index;
  uint64_t last_log_term = log_entries_.back().term;
  resp.set_previouslogindex(last_log_index);
  resp.set_previouslogterm(last_log_term);
  log_mutex_.releaseReadLock();
  bool is_candidate_log_newer = req.previouslogterm() > last_log_term ||
                                (req.previouslogterm() == last_log_term &&
                                 req.previouslogindex() >= last_log_index);
  last_vote_request_term_ =
      MAX(static_cast<uint64_t>(last_vote_request_term_), req.term());
  {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    if (req.term() > current_term_ && is_candidate_log_newer) {
      resp.set_vote(true);
      current_term_ = req.term();
      leader_id_ = PeerId();
      if (state_ == State::LEADER) {
        follower_trackers_run_ = false;
        entry_replicated_signal_.notify_all();
      }
      state_ = State::FOLLOWER;
      VLOG(1) << "Peer " << PeerId::self().ipPort() << " is voting for "
              << request.sender() << " in term " << current_term_;
    } else {
      VLOG(1) << "Peer " << PeerId::self().ipPort() << " is declining vote for "
              << request.sender() << " in term " << req.term() << ". Reason: "
              << (req.term() > current_term_ ? "" : "Term is equal or less. ")
              << (is_candidate_log_newer ? "" : "Log is older. ");
      resp.set_vote(false);
    }
  }

  response->impose<kVoteResponse>(resp);
  {
    std::lock_guard<std::mutex> lock(last_heartbeat_mutex_);
    last_heartbeat_ = std::chrono::system_clock::now();
  }
  election_timeout_ = setElectionTimeout();
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
    VLOG(3) << "AppendEntries failed for peer " << peer.ipPort();
    return false;
  }
}

int RaftNode::sendRequestVote(const PeerId& peer, uint64_t term,
                              uint64_t last_log_index, uint64_t last_log_term) {
  Message request, response;
  proto::RequestVote vote_request;
  vote_request.set_term(term);
  vote_request.set_commitindex(commit_index_);
  vote_request.set_previouslogindex(last_log_index);
  vote_request.set_previouslogterm(last_log_term);
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

void RaftNode::stateManagerThread() {
  TimePoint last_hb_time;
  bool election_timeout = false;
  State state;
  uint64_t current_term;
  PeerId leader_id;
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
      leader_id = leader_id_;
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
      uint16_t a = 0;
      while (follower_trackers_run_) {
        // call commit new entries
        ++a;
        if (a % 4 == 0) {
          appendLogEntry(19);
        }

        // VLOG(1) << "See if something is there to commit";
        commitReplicatedEntries();
        if (follower_trackers_run_) {
          entry_replicated_signal_.wait_for(wait_lock,
                                            std::chrono::milliseconds(20));
        }
        if (a > 1000) {
          std::lock_guard<std::mutex> state_lock(state_mutex_);
          if (state_ == State::LEADER) {
            follower_trackers_run_ = false;
            entry_replicated_signal_.notify_all();
          }
          state_ = State::FOLLOWER;
          usleep(30000);
          std::unique_lock<std::mutex> heartbeat_lock(last_heartbeat_mutex_);
          last_heartbeat_ = std::chrono::system_clock::now();
        }
      }
      VLOG(1) << "Peer " << PeerId::self() << " Lost leadership. ";
      for (std::thread& follower_thread : follower_trackers_) {
        follower_thread.join();
      }
      follower_trackers_.clear();
      VLOG(1) << "Peer " << PeerId::self() << ": Follower trackers closed. ";
    }
  }  // while(!is_exiting_)
  state_thread_running_ = false;
}

void RaftNode::conductElection() {
  uint16_t num_votes = 0;
  std::unique_lock<std::mutex> state_lock(state_mutex_);
  state_ = State::CANDIDATE;
  uint64_t old_term = current_term_;
  uint64_t term = ++current_term_;
  if (last_vote_request_term_ >= current_term_) {
    current_term_ = last_vote_request_term_ + 1;
    term = current_term_;
  }
  leader_id_ = PeerId();
  state_lock.unlock();
  log_mutex_.acquireReadLock();
  uint64_t last_log_index = log_entries_.back().index;
  uint64_t last_log_term = log_entries_.back().term;
  log_mutex_.releaseReadLock();

  VLOG(1) << "Peer " << PeerId::self() << " is an election candidate for term "
          << term;

  std::vector<std::future<int>> responses;
  for (const PeerId& peer : peer_list_) {
    std::future<int> p =
        std::async(std::launch::async, &RaftNode::sendRequestVote, this, peer,
                   term, last_log_index, last_log_term);
    responses.push_back(std::move(p));
  }
  for (std::future<int>& response : responses) {
    if (response.get() == VOTE_GRANTED) {
      ++num_votes;
    } else {
      // TODO(aqurai): Handle non-responding peers
    }
  }

  state_lock.lock();
  if (state_ == State::CANDIDATE && num_votes >= peer_list_.size() / 2) {
    // This peer wins the election.
    state_ = State::LEADER;
    leader_id_ = PeerId::self();
    // Renew election timeout every session.
    election_timeout_ = setElectionTimeout();
  } else {
    // This peer doesn't win the election.
    state_ = State::FOLLOWER;
    leader_id_ = PeerId();
    // Set a longer election timeout if the candidate loses election to prevent
    // from holding elections and getting rejected repeatedly in consecutive
    // terms (due to less updated log) and blocking other peers from holding
    // election.
    election_timeout_ = 4 * setElectionTimeout();
  }
  state_lock.unlock();
  std::unique_lock<std::mutex> heartbeat_lock(last_heartbeat_mutex_);
  last_heartbeat_ = std::chrono::system_clock::now();
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

    while (!append_successs && follower_trackers_run_) {
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

        if (it == log_entries_.end()) {
          int a = 0;
          // for(LogEntry e : log_entries_) {
          for (std::vector<LogEntry>::iterator ii = log_entries_.begin();
               ii != log_entries_.end(); ++ii) {
            VLOG(1) << PeerId::self() << ": Element index " << ii->index
                    << " -- cnt = " << a;
            a++;
          }
          VLOG(1) << PeerId::self() << ": Num Logs =  " << log_entries_.size();
          VLOG(1) << PeerId::self() << ": follower_next_index "
                  << follower_next_index;

          std::stringstream logss;
          logss << " ********************* \n";
          for (std::string str : loglog) {
            logss << str << " ::\n";
          }
          logss << "*************************** \n";

          VLOG(1) << "loglog print : ";

          VLOG(1) << logss.str();

          CHECK(it != log_entries_.end());
        }

        append_entries.set_newentry(it->entry);
        append_entries.set_newentryterm(it->term);
        // TODO(aqurai): verify that (it-1) never throws an error.
        append_entries.set_previouslogindex((it - 1)->index);
        append_entries.set_previouslogterm((it - 1)->term);
      }

      append_entries.set_commitindex(commit_index_);
      log_mutex_.releaseReadLock();

      if (!sendAppendEntries(peer, append_entries, &append_response)) {
        VLOG(1) << PeerId::self() << ": Flr trkr C - failed sendAppendEntries to " << peer;
        continue;
      }

      follower_commit_index = append_response.commitindex();
      append_successs = append_response.success();
      if (!append_successs) {
        // Append on follower failed due to a conflict. Send an older entry
        // and try again.
        CHECK_GT(follower_next_index, 1);
        --follower_next_index;
        if (follower_commit_index >= follower_next_index) {
          // This should not happen.
          LOG(WARNING) << PeerId::self()
                       << ": Conflicting entry already committed on peer "
                       << peer << ". Peer commit index "
                       << follower_commit_index
                       << ". Peer last log index, term "
                       << append_response.previouslogindex() << ", "
                       << append_response.previouslogterm() << ". "
                       << "Leader last log index, term "
                       << append_entries.previouslogindex() << ", "
                       << append_entries.previouslogterm() << ". ";
        }
      } else {
        if (follower_next_index == append_response.previouslogindex()) {
          // The response is from an append entry RPC, not a regular heartbeat.
          log_mutex_.acquireWriteLock();
          std::vector<LogEntry>::iterator it =
              getIteratorByIndex(follower_next_index);
          ++it->replication_count;
          VLOG_IF(1, it->index % 20 == 0 &&
                         it->replication_count == peer_list_.size())
              << "********* Entry " << it->index << " replicated on all peers";
          log_mutex_.releaseWriteLock();
          ++follower_next_index;
          entry_replicated_signal_.notify_all();
        }
      }
    }  //  while (!append_successs && follower_trackers_run_)

    // Todo: see if this condition check is necessary
    if (follower_trackers_run_) {
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

// Assumes at least read lock is acquired for log_mutex_
std::vector<RaftNode::LogEntry>::iterator RaftNode::getIteratorByIndex(
    uint64_t index) {
  std::vector<LogEntry>::iterator it = log_entries_.end();
  if (index < log_entries_.front().index || index > log_entries_.back().index) {
    return it;
  } else {
    // The log indexes are always sequential.
    it = log_entries_.begin() + (index - log_entries_.front().index);
    // todo: remove before commit
    if (it != log_entries_.end()) {
      LOG_IF(FATAL, it->index != index) << PeerId::self()
                                        << ": it->index: " << it->index
                                        << ", index arg: " << index << ".  "
                                        << " log front index = "
                                        << log_entries_.front().index
                                        << "Log begin iter index = "
                                        << log_entries_.begin()->index;
      CHECK(it->index == index);
    }
    return it;
  }
}

uint64_t RaftNode::appendLogEntry(uint32_t entry) {
  uint64_t current_term;
  {
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    if (state_ != State::LEADER) {
      return 0;
    }
    current_term = current_term_;
  }
  log_mutex_.acquireWriteLock();
  LogEntry new_entry;
  new_entry.index = log_entries_.back().index + 1;
  new_entry.term = current_term;
  new_entry.entry = entry;
  new_entry.replication_count = 0;
  log_entries_.push_back(new_entry);

  std::stringstream s;
  s << "Add log entry D: index, term, count = " << new_entry.index << ", "
    << new_entry.term << ", " << log_entries_.size();
  loglog.push_back(s.str());

  log_mutex_.releaseWriteLock();
  new_entries_signal_.notify_all();
  VLOG_IF(1, new_entry.index % 10 == 0) << "Adding entry to log with index "
                                        << new_entry.index;
  return new_entry.index;
}

void RaftNode::commitReplicatedEntries() {
  log_mutex_.acquireReadLock();
  std::vector<LogEntry>::iterator it = getIteratorByIndex(commit_index_ + 1);
  if (it != log_entries_.end()) {
    // Todo before commit - remove?
    if (!(it->replication_count <= peer_list_.size())) {
      LOG(ERROR) << "Replication count (" << it->replication_count
                 << ") is higher than peer size (" << peer_list_.size()
                 << ") at peer " << PeerId::self();
    }
    if (it->replication_count > peer_list_.size() / 2) {
      // Replicated on more than half of the peers.
      ++commit_index_;
      VLOG_IF(1, commit_index_ % 10 == 0)
          << PeerId::self() << ": Commit index increased to " << commit_index_
          << " With replication count " << it->replication_count
          << " and with term " << it->term;
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
