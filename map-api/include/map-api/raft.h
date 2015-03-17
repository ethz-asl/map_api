/* Notes:
 * Things raft should be doing
 *    - Send heartbeats to all peers if leader
 *    - Handle heartbeat timeouts if follower
 *    - Heartbeats from leaders include logical time, term number
 *    - Handle RPC from clients
 *
 *
 * One thread for handling heartbeats and state
 * One thread for handling RPCs
 *
 * CURRENT ASSUMPTIONS:
 * - A peer can reach all other peers, or none.
 *    i.e, no network partitions, and no case where a peer can contact some
 *    peers and not others.
 * - No malicious peers!
 *
 * --------------------------------------------------------------
 *  TODO List
 * --------------------------------------------------------------
 *
 * PENDING: Send heartbeat is blocking. the cycle takes longer if there
 *  are many peers. This may cause problems of timeout when there are a
 *  lot of peers. Is there a Way to send heartbeat and not wait for response?
 *  UDP style?
 * PENDING: Handle peers who dont respond to vote rpc
 * PENDING: Values for timeout
 * PENDING: Impl good way to get peer list, remove non responding peers
 * PENDING: Vote RPC is now sent one by one. can parallelize?
 * PENDING: Peer handling, adding, removing
 * PENDING: When to start raft server
 * PENDING: Multiple raft instances managed by a manager class
 * PENDING: Remove the extra log messages
 * PENDING: std::async vs std::thread+join for heartbeat
 *
 * DONE: Protobuf for messages, including heatbeat. Should have fields:
 *  term index, logical time
 * DONE: Change leader to follower if heartbeat not ack'd.
 * DEFERRED: MIGHT NOT BE NECESSARY: Make a heartbeat struct (atomic) having
 *   rcvd time, term, sender id, contained logical time. Write struct in handler
 *   func. read and act on state in heartbeat thread.
 * DONE: Accessing atomic state_ member within lock in heartbeat thread.
 *   verify if there will be deadlocks!
 * DONE: Handle RPC in a separate thread? See how listener thread in hub
 *  invokes callback in main thread. Turns out, it is exec in the hub's
 *  listener thread.??
 * Not needed: Atomic or lock for election timeout member var
 * DONE: can discard last_correct_hb_time in the thread? - no, an extra variable
 *    is needed anyway, for send heartbeat time.
 *
 * DONE: Check if logical time check in requestVote is ok. Because
 *  Logical time can increase due to other messages after a candidate sends
 *  requestvote rpc. OR may be compare it with last heartbeat logical time.
 *  -- Not ok.
 */

#ifndef MAP_API_RAFT_H_
#define MAP_API_RAFT_H_

#include <atomic>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "map-api/message.h"
#include "map-api/peer-id.h"

namespace map_api {

class RaftCluster {
 public:
  enum class State {
    LEADER,
    FOLLOWER,
    CANDIDATE,
    DISCONNECTING
  };

  static RaftCluster& instance();

  void registerHandlers();

  void start();
  bool isRunning() const { return heartbeat_thread_running_; }

  static void staticHandleHearbeat(const Message& request, Message* response);
  static void staticHandleRequestVote(const Message& request,
                                      Message* response);

  // ONLY FOR TEST. REMOVE LATER.
  void addPeerBeforeStart(PeerId peer) { peer_list_.insert(peer); }

  static const char kHeartbeat[];
  static const char kVoteRequest[];
  static const char kVoteResponse[];

 private:
  /**
   * Singleton class. Will have a singleton manager class later, for managing
   * multiple raft instances per peer.
   */
  RaftCluster();
  RaftCluster(const RaftCluster&) = delete;
  RaftCluster& operator=(const RaftCluster&) = delete;
  ~RaftCluster();

  /**
   * ========
   * Handlers
   * ========
   */
  void handleHearbeat(const Message& request, Message* response);
  void handleRequestVote(const Message& request, Message* response);

  /**
   * ==================================================
   * RPCs for heartbeat, leader election, transactions
   * ==================================================
   */

  /**
   *
   * @param id
   * @param term
   * @return True if RPC is acknowledged.
   */
  bool sendHeartbeat(PeerId id, uint64_t term);

  /**
   *
   * @param id
   * @param term
   * @return 1 if vote granted, 0 if vote denied, -1 if peer unreachable.
   */
  int sendRequestVote(PeerId id, uint64_t term);

  /**
   * ===============================
   * Leader election related members
   * ===============================
   */
  // State information
  PeerId leader_id_;
  State state_;
  uint64_t current_term_;
  bool leader_known_;
  std::mutex state_mutex_;

  // Heartbeat information
  typedef std::chrono::time_point<std::chrono::system_clock> TimePoint;
  TimePoint last_heartbeat_;
  PeerId last_heartbeat_sender_;
  uint64_t last_heartbeat_sender_term_;
  std::mutex last_heartbeat_mutex_;

  // Heartbeat monitoring/sending, state changes managed in a separate thread.
  static void heartbeatThread(RaftCluster* thisRaftCluster);
  std::thread heartbeat_thread_;  // kill in destructor
  std::atomic<bool> heartbeat_thread_running_;
  std::atomic<bool> is_exiting_;

  std::set<PeerId> peer_list_;

  double election_timeout_;  // A random value between 50 and 150 ms
  static void setElectionTimeout();
};

}  // namespace map_api

#endif  // MAP_API_RAFT_H_
