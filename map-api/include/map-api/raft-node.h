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

#include "map-api/peer-id.h"

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
  inline bool isRunning() const { return heartbeat_thread_running_; }
  uint64_t term() const;
  const PeerId& leader() const;
  State state() const;
  inline PeerId self_id() const { return PeerId::self(); }

  static void staticHandleHeartbeat(const Message& request, Message* response);
  static void staticHandleRequestVote(const Message& request,
                                      Message* response);

  static const char kHeartbeat[];
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
  void handleHeartbeat(const Message& request, Message* response);
  void handleRequestVote(const Message& request, Message* response);

  // ====================================================
  // RPCs for heartbeat, leader election, log replication
  // ====================================================
  bool sendHeartbeat(const PeerId& peer, uint64_t term);

  enum {
    VOTE_GRANTED,
    VOTE_DECLINED,
    FAILED_REQUEST
  };
  int sendRequestVote(const PeerId& peer, uint64_t term);

  // =====
  // State
  // =====

  // State information.
  PeerId leader_id_;
  State state_;
  uint64_t current_term_;
  mutable std::mutex state_mutex_;

  // Heartbeat information.
  typedef std::chrono::time_point<std::chrono::system_clock> TimePoint;
  TimePoint last_heartbeat_;
  std::mutex last_heartbeat_mutex_;

  void heartbeatThread();
  std::thread heartbeat_thread_;  // Gets joined in destructor.
  std::atomic<bool> heartbeat_thread_running_;
  std::atomic<bool> is_exiting_;

  std::set<PeerId> peer_list_;

  // ===============
  // Leader election
  // ===============

  int election_timeout_;  // A random value between 50 and 150 ms.
  void conductElection();
  void followerTracker(const PeerId& peer, uint64_t term);


  // =====================
  // Log entries/revisions
  // =====================

  struct LogEntry {
    uint16_t index;
    uint64_t term;
    uint16_t logicaltime;
    uint16_t entry;
  };
  std::vector<LogEntry> uncommitted_log_;
  std::pair<uint16_t, uint16_t> final_result_;

  // Started when leadership is acquired. Gets killed when leadership is lost.
  std::vector<std::thread> follower_trackers_;
  std::atomic<bool> follower_trackers_run_;
  static int setElectionTimeout();
};
}  // namespace map_api

#endif  // MAP_API_RAFT_NODE_H_
