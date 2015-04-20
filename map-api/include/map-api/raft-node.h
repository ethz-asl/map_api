/* Notes:
 * Things raft should be doing
 *    - Send heartbeats to all peers if leader
 *    - Handle heartbeat timeouts if follower, hold election
 *    - Heartbeats from leaders include term number, log entry info
 *    - Handle RPC from clients
 *    - Send new log entries/chunk revisions to all peers
 *
 * CURRENT ASSUMPTIONS:
 * - A peer can reach all other peers, or none.
 *    i.e, no network partitions, and no case where a peer can contact some
 *    peers and not others.
 * - No malicious peers!
 *
 * -------------------------
 * Lock acquisition ordering
 * -------------------------
 * 1. state_mutex_
 * 2. log_mutex_
 * 3. commit_mutex_
 * 4. peer_mutex_
 * 5. follower_tracker_mutex_
 * 6. last_heartbeat_mutex_
 * 
 * --------------------------------------------------------------
 *  TODO List at this point
 * --------------------------------------------------------------
 *
 * PENDING: Handle peers who don't respond to vote rpc
 * PENDING: Values for timeout
 * PENDING: Adding and removing peers, handling non-responding peers
 * PENDING: Multiple raft instances managed by a manager class
 * PENDING: Remove the extra log messages
 */

#ifndef MAP_API_RAFT_NODE_H_
#define MAP_API_RAFT_NODE_H_

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest_prod.h>

#include "./raft.pb.h"
#include "map-api/peer-id.h"
#include "map-api/reader-writer-lock.h"

namespace map_api {
class Message;

// Implementation of Raft consensus algorithm presented here:
// https://raftconsensus.github.io, http://ramcloud.stanford.edu/raft.pdf
class RaftNode {
 public:
  enum class State {
    JOINING,
    LEADER,
    FOLLOWER,
    CANDIDATE,
    DISCONNECTING
  };

  static RaftNode& instance();
  void kill();

  void registerHandlers();

  void start();
  inline bool isRunning() const { return state_thread_running_; }
  uint64_t term() const;
  const PeerId& leader() const;
  State state() const;
  inline PeerId self_id() const { return PeerId::self(); }

  // Returns index of the appended entry if append succeeds, or zero otherwise
  uint64_t leaderAppendLogEntry(uint32_t entry);

  static void staticHandleAppendRequest(const Message& request,
                                        Message* response);
  static void staticHandleRequestVote(const Message& request,
                                      Message* response);
  static void staticHandleJoinQuitRequest(const Message& request,
                                        Message* response);
  static void staticHandleNotifyJoinQuitSuccess(const Message& request,
                                                Message* response);

  static const char kAppendEntries[];
  static const char kAppendEntriesResponse[];
  static const char kVoteRequest[];
  static const char kVoteResponse[];
  static const char kAddRemovePeer[];
  static const char kJoinQuitRequest[];
  static const char kJoinQuitResponse[];
  static const char kNotifyJoinQuitSuccess[];

 private:
  FRIEND_TEST(ConsensusFixture, DISABLED_LeaderElection);
  // TODO(aqurai) Only for test, will be removed later.
  inline void addPeerBeforeStart(PeerId peer) {
    peer_list_.insert(peer);
      ++num_peers_;
  }
  bool giveUpLeadership();

  // Singleton class. There will be a singleton manager class later,
  // for managing multiple raft instances per peer.
  RaftNode();
  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  // ========
  // Handlers
  // ========
  void handleAppendRequest(const Message& request, Message* response);
  void handleRequestVote(const Message& request, Message* response);
  void handleJoinQuitRequest(const Message& request, Message* response);
  void handleNotifyJoinQuitSuccess(const Message& request, Message* response);

  // ====================================================
  // RPCs for heartbeat, leader election, log replication
  // ====================================================
  bool sendAppendEntries(const PeerId& peer,
                         const proto::AppendEntriesRequest& append_entries,
                         proto::AppendEntriesResponse* append_response);
  enum class VoteResponse {
    VOTE_GRANTED,
    VOTE_DECLINED,
    VOTER_NOT_ELIGIBLE,
    FAILED_REQUEST
  };
  VoteResponse sendRequestVote(const PeerId& peer, uint64_t term,
                      uint64_t last_log_index, uint64_t last_log_term);
  proto::JoinQuitResponse sendJoinQuitRequest(const PeerId& peer,
                                              proto::PeerRequestType type);
  void sendNotifyJoinQuitSuccess(const PeerId& peer);

  // ================
  // State Management
  // ================

  // State information.
  PeerId leader_id_;
  State state_;
  uint64_t current_term_;
  mutable std::mutex state_mutex_;

  // Heartbeat information.
  typedef std::chrono::time_point<std::chrono::system_clock> TimePoint;
  TimePoint last_heartbeat_;
  std::mutex last_heartbeat_mutex_;
  inline void updateHeartbeatTime() {
    std::lock_guard<std::mutex> heartbeat_lock(last_heartbeat_mutex_);
    last_heartbeat_ = std::chrono::system_clock::now();
  }
  inline double getTimeSinceHeartbeatMs() {
    std::lock_guard<std::mutex> lock(last_heartbeat_mutex_);
    TimePoint last_hb_time = last_heartbeat_;
    TimePoint now = std::chrono::system_clock::now();
    return static_cast<double>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now - last_hb_time).count());
  }

  std::thread state_manager_thread_;  // Gets joined in destructor.
  std::atomic<bool> state_thread_running_;
  std::atomic<bool> is_exiting_;
  void stateManagerThread();

  // ===============
  // Peer management
  // ===============

  enum PeerStatus {
    JOINING,
    AVAILABLE,
    NOT_RESPONDING,
    ANNOUNCED_DISCONNECTING,
    OFFLINE
  };

  struct FollowerTracker {
    std::thread tracker_thread;
    std::atomic<bool> tracker_run;
    std::atomic<uint64_t> replication_index;
    std::atomic<PeerStatus> status;
  };

  typedef std::unordered_map<PeerId, std::shared_ptr<FollowerTracker>> TrackerMap;
  // One tracker thread is started for each peer when leadership is acquired.
  // They get joined when leadership is lost or corresponding peer disconnects.
  TrackerMap follower_tracker_map_;

  // Available peers. Modified ONLY in followerCommitNewEntries() or
  // leaderCommitReplicatedEntries() or leaderMonitorFollowerStatus()
  std::set<PeerId> peer_list_;
  std::atomic<uint> num_peers_;
  std::mutex peer_mutex_;
  std::mutex follower_tracker_mutex_;

  // Expects follower_tracker_mutex_ locked.
  void leaderShutDownTracker(const PeerId& peer);
  void leaderShutDownAllTrackes();
  void leaderLaunchTracker(const PeerId& peer, uint64_t current_term);

  // Expects no lock to be taken.
  void leaderMonitorFollowerStatus(uint64_t current_term);
  void leaderAddRemovePeer(const PeerId& peer, proto::PeerRequestType request,
                           uint64_t current_term);
  void followerAddRemovePeer(const proto::AddRemovePeer& add_remove_peer);

  // First time join.
  std::atomic<bool> is_join_notified_;
  std::atomic<uint64_t> join_log_index_;
  PeerId join_request_peer_;
  void joinRaft();

  // ===============
  // Leader election
  // ===============

  enum {
    VOTE_GRANTED,
    VOTE_DECLINED,
    FAILED_REQUEST
  };
  std::atomic<int> election_timeout_ms_;  // A random value between 50 and 150 ms.
  static int setElectionTimeout();     // Set a random election timeout value.
  void conductElection();

  std::atomic<bool> follower_trackers_run_;
  std::atomic<uint64_t> last_vote_request_term_;
  void followerTrackerThread(const PeerId& peer, uint64_t term,
                             const std::shared_ptr<FollowerTracker> my_tracker);

  // =====================
  // Log entries/revisions
  // =====================
  // Index will always be sequential, unique.
  // Leader will overwrite follower logs where index+term doesn't match.

  // In Follower state, only handleAppendRequest writes to log_entries.
  // In Leader state, only appendLogEntry writes to log entries.
  std::vector<std::shared_ptr<proto::RaftRevision>> log_entries_;
  std::condition_variable new_entries_signal_;
  ReaderWriterMutex log_mutex_;
  typedef std::vector<std::shared_ptr<proto::RaftRevision>>::iterator
      LogIterator;

  // Assumes at least read lock is acquired for log_mutex_.
  LogIterator getLogIteratorByIndex(uint64_t index);

  // Expects write lock for log_mutex to be acquired.
  uint64_t leaderAddEntryToLog(uint32_t entry, uint32_t current_term,
                               const PeerId& peer_id,
                               proto::PeerRequestType request_type =
                                   proto::PeerRequestType::ADD_PEER);

  // The two following methods assume write lock is acquired for log_mutex_.
  proto::AppendResponseStatus followerAppendNewEntries(
      proto::AppendEntriesRequest& request);
  void followerCommitNewEntries(const proto::AppendEntriesRequest& request,
                                State state);
  void setAppendEntriesResponse(proto::AppendResponseStatus status,
                                proto::AppendEntriesResponse* response);

  // Expects locks for commit_mutex_ and log_mutex_to NOT have been acquired.
  void leaderCommitReplicatedEntries(uint64_t current_term);

  uint64_t commit_index_;
  uint64_t committed_result_;
  mutable std::mutex commit_mutex_;
  const uint64_t& commit_index() const;
  const uint64_t& committed_result() const;
};
}  // namespace map_api

#endif  // MAP_API_RAFT_NODE_H_
