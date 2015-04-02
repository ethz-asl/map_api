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
 * --------------------------------------------------------------
 *  TODO List at this point
 * --------------------------------------------------------------
 *
 * PENDING: Handle peers who dont respond to vote rpc
 * PENDING: Values for timeout
 * PENDING: Impl good way to get peer list, remove non responding peers
 * PENDING: Vote RPC is now sent one by one. can parallelize?
 * PENDING: Peer handling, adding, removing
 * PENDING: Multiple raft instances managed by a manager class
 * PENDING: Remove the extra log messages
 * PENDING: Change leader to follower if many heartbeats not ack'd?
 *
 * PENDING: Rename heartbeatThread to stateManager?
 * PENDING: Handle append entry on follower
 * PENDING: send appendentries repeatedly if failed to reach
 * PENDING: add a (0,0) default log entry instead of checking isEmpty in
 *follower trackers
 * PENDING: possible to read index -1 in log_entry_ vector in follower tracker
 * PENDING: Necessary to replicate all log? allow replication of partial log for
 *new peers.
 * PENDING: Multiple entries at once, and send at most 10 entries at a time
 * PENDING: while (!append_successs) can run indefinitely in a bad case
 * PENDING: At leader, handle if follower responds with higher term.
 * PENDING: failure of sendAppendEntries (i.e. non responding peer)
 * PENDING: remove commit count member. better way to store commit count
 *
 */

#ifndef MAP_API_RAFT_NODE_H_
#define MAP_API_RAFT_NODE_H_

#include <atomic>
#include <condition_variable>
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
class ReaderWriterMutex;

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
  uint64_t appendLogEntry(uint32_t entry);

  static void staticHandleHeartbeat(const Message& request, Message* response);
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
  bool sendHeartbeat(const PeerId& peer, uint64_t term);
  bool sendAppendEntries(const PeerId& peer,
                         proto::AppendEntriesRequest& append_entries,
                         proto::AppendEntriesResponse* append_response);

  enum {
    VOTE_GRANTED,
    VOTE_DECLINED,
    FAILED_REQUEST
  };
  int sendRequestVote(const PeerId& peer, uint64_t term,
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

  std::atomic<int> election_timeout_;  // A random value between 50 and 150 ms.
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
  // Presently sending all log entries to everyone, but later, just send latest
  // revision and new log entries to new peers.

  struct LogEntry {
    uint64_t index;
    uint64_t term;
    uint32_t entry;
    std::set<PeerId> replicator_peers;
  };

  // In Follower state, only handleAppendRequest writes to log_entries.
  // In Leader state, only appendLogEntry writes to log entries.
  std::vector<LogEntry> log_entries_;
  std::pair<uint64_t, uint64_t> committed_result_;
  std::condition_variable new_entries_signal_;
  std::condition_variable entry_replicated_signal_;
  ReaderWriterMutex log_mutex_;

  std::atomic<uint64_t> commit_index_;

  struct FollowerStatus {
    uint64_t next_index;
    uint64_t commit_index;
  };

  std::vector<LogEntry>::iterator getIteratorByIndex(uint64_t index);

  // After a new entry is replicated on followers,
  // checks if some entries can be committed.
  void commitReplicatedEntries();
};
}  // namespace map_api

#endif  // MAP_API_RAFT_NODE_H_
