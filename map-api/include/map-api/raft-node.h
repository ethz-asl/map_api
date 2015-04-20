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
 * 4. last_heartbeat_mutex_
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
#include <map>
#include <mutex>
#include <set>
#include <thread>
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
    LEADER,
    FOLLOWER,
    CANDIDATE,
    DISCONNECTING
  };

  static RaftNode& instance();

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

  static const char kAppendEntries[];
  static const char kAppendEntriesResponse[];
  static const char kVoteRequest[];
  static const char kVoteResponse[];

 private:
  FRIEND_TEST(ConsensusFixture, DISABLED_LeaderElection);
  // TODO(aqurai) Only for test, will be removed later.
  inline void addPeerBeforeStart(PeerId peer) { peer_list_.insert(peer); }
  bool giveUpLeadership();

  // Singleton class. There will be a singleton manager class later,
  // for managing multiple raft instances per peer.
  RaftNode();
  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;
  ~RaftNode();

  // ========
  // Handlers
  // ========
  void handleAppendRequest(const Message& request, Message* response);
  void handleRequestVote(const Message& request, Message* response);

  // ====================================================
  // RPCs for heartbeat, leader election, log replication
  // ====================================================
  bool sendAppendEntries(const PeerId& peer,
                         const proto::AppendEntriesRequest& append_entries,
                         proto::AppendEntriesResponse* append_response);

  enum class VoteResponse {
    VOTE_GRANTED,
    VOTE_DECLINED,
    FAILED_REQUEST
  };
  VoteResponse sendRequestVote(const PeerId& peer, uint64_t term,
                      uint64_t last_log_index, uint64_t last_log_term);

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

  std::thread state_manager_thread_;  // Gets joined in destructor.
  std::atomic<bool> state_thread_running_;
  std::atomic<bool> is_exiting_;
  void stateManagerThread();

  // ===============
  // Peer management
  // ===============

  std::set<PeerId> peer_list_;

  // ===============
  // Leader election
  // ===============

  std::atomic<int> election_timeout_ms_;  // A random value between 50 and 150 ms.
  static int setElectionTimeout();     // Set a random election timeout value.
  void conductElection();

  // Started when leadership is acquired. Gets killed when leadership is lost.
  std::vector<std::thread> follower_trackers_;
  std::atomic<bool> follower_trackers_run_;
  std::atomic<uint64_t> last_vote_request_term_;
  void followerTrackerThread(const PeerId& peer, uint64_t term);

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

  // The two following methods assume write lock is acquired for log_mutex_.
  proto::AppendResponseStatus followerAppendNewEntries(
      proto::AppendEntriesRequest& request);
  void followerCommitNewEntries(const proto::AppendEntriesRequest& request);
  void setAppendEntriesResponse(proto::AppendResponseStatus status,
                                proto::AppendEntriesResponse* response);

  // Expects locks for commit_mutex_ and log_mutex_to NOT have been acquired.
  void leaderCommitReplicatedEntries();

  std::map<PeerId, std::unique_ptr<std::atomic<uint64_t>>>
      peer_replication_indices_;
  typedef std::map<PeerId, std::unique_ptr<std::atomic<uint64_t>>>::iterator
      ReplicationIterator;

  uint64_t commit_index_;
  uint64_t committed_result_;
  mutable std::mutex commit_mutex_;
  const uint64_t& commit_index() const;
  const uint64_t& committed_result() const;
};
}  // namespace map_api

#endif  // MAP_API_RAFT_NODE_H_