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
 * 3. peer_mutex_
 * 4. follower_tracker_mutex_
 * 5. last_heartbeat_mutex_
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
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest_prod.h>
#include <multiagent-mapping-common/unique-id.h>

#include "./raft.pb.h"
#include "map-api/peer-id.h"
#include "map-api/revision.h"
#include "multiagent-mapping-common/reader-writer-lock.h"
#include "map-api/raft-chunk-data-ram-container.h"

namespace map_api {
class Message;
class RaftChunk;

// Implementation of Raft consensus algorithm presented here:
// https://raftconsensus.github.io, http://ramcloud.stanford.edu/raft.pdf
class RaftNode {
 public:
  enum class State {
    JOINING,
    LEADER,
    FOLLOWER,
    CANDIDATE,
    LOST_CONNECTION,
    DISCONNECTING,
    STOPPED
  };

  void start();
  void stop();
  inline bool isRunning() const { return state_thread_running_; }
  uint64_t getTerm() const;
  const PeerId& getLeader() const;
  State getState() const;

  // Returns index of the appended entry if append succeeds, or zero otherwise
  uint64_t leaderAppendLogEntry(
      const std::shared_ptr<proto::RaftLogEntry>& new_entry);

  static const char kAppendEntries[];
  static const char kAppendEntriesResponse[];
  static const char kChunkLockRequest[];
  static const char kChunkLockResponse[];
  static const char kChunkUnlockRequest[];
  static const char kChunkUnlockResponse[];
  static const char kInsertRequest[];
  static const char kUpdateRequest[];
  static const char kInsertResponse[];
  static const char kVoteRequest[];
  static const char kVoteResponse[];
  static const char kLeaveRequest[];
  static const char kLeaveNotification[];
  static const char kRaftChunkRequestResponse[];
  static const char kQueryState[];
  static const char kQueryStateResponse[];
  static const char kConnectRequest[];
  static const char kConnectResponse[];
  static const char kInitRequest[];

 private:
  friend class ConsensusFixture;
  friend class RaftChunk;
  FRIEND_TEST(ConsensusFixture, LeaderElection);
  RaftNode();
  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  bool giveUpLeadership();

  typedef RaftChunkDataRamContainer::RaftLog::iterator LogIterator;
  typedef RaftChunkDataRamContainer::RaftLog::const_iterator ConstLogIterator;
  typedef RaftChunkDataRamContainer::LogReadAccess LogReadAccess;
  typedef RaftChunkDataRamContainer::LogWriteAccess LogWriteAccess;

  // ========
  // Handlers
  // ========
  // Raft requests.
  void handleAppendRequest(proto::AppendEntriesRequest* request,
                           const PeerId& sender, Message* response);
  void handleRequestVote(const proto::VoteRequest& request,
                         const PeerId& sender, Message* response);
  void handleQueryState(const proto::QueryState& request, Message* response);

  // Chunk Requests.
  void handleConnectRequest(const PeerId& sender, Message* response);
  void handleLeaveRequest(const PeerId& sender, uint64_t serial_id,
                          Message* response);
  void handleChunkLockRequest(const PeerId& sender, uint64_t serial_id,
                              Message* response);
  void handleChunkUnlockRequest(const PeerId& sender, uint64_t serial_id,
                                uint64_t lock_index, bool proceed_commits,
                                Message* response);
  void handleInsertRequest(proto::InsertRequest* request,
                           const PeerId& sender, Message* response);

  // Not ready if entries from older leader pending commit.
  inline bool checkReadyToHandleChunkRequests() const;

  // ====================================================
  // RPCs for heartbeat, leader election, log replication
  // ====================================================
  bool sendAppendEntries(const PeerId& peer,
                         proto::AppendEntriesRequest* append_entries,
                         proto::AppendEntriesResponse* append_response);
  enum class VoteResponse {
    VOTE_GRANTED,
    VOTE_DECLINED,
    VOTER_NOT_ELIGIBLE,
    FAILED_REQUEST
  };
  VoteResponse sendRequestVote(const PeerId& peer, uint64_t term,
                               uint64_t last_log_index, uint64_t last_log_term,
                               uint64_t current_commit_index) const;

  bool sendInitRequest(const PeerId& peer, const LogWriteAccess& log_writer);

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
  mutable TimePoint last_heartbeat_;
  mutable std::mutex last_heartbeat_mutex_;
  inline void updateHeartbeatTime() const {
    std::lock_guard<std::mutex> heartbeat_lock(last_heartbeat_mutex_);
    last_heartbeat_ = std::chrono::system_clock::now();
  }
  inline double getTimeSinceHeartbeatMs() {
    TimePoint last_hb_time;
    {
      std::lock_guard<std::mutex> lock(last_heartbeat_mutex_);
      last_hb_time = last_heartbeat_;
    }
    TimePoint now = std::chrono::system_clock::now();
    return static_cast<double>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now - last_hb_time).count());
  }

  std::thread state_manager_thread_;  // Gets joined in destructor.
  std::atomic<bool> state_thread_running_;
  std::atomic<bool> is_exiting_;
  std::atomic<bool> leave_requested_;
  void stateManagerThread();

  // ===============
  // Peer management
  // ===============

  enum class PeerStatus {
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
  bool hasPeer(const PeerId& peer);

  // Expects follower_tracker_mutex_ locked.
  void leaderShutDownTracker(const PeerId& peer);
  void leaderShutDownAllTrackes();
  void leaderLaunchTracker(const PeerId& peer, uint64_t current_term);

  // Expects no lock to be taken.
  void leaderMonitorFollowerStatus(uint64_t current_term);
  void leaderAddPeer(const PeerId& peer, const LogWriteAccess& log_writer,
                     uint64_t current_term);
  void leaderRemovePeer(const PeerId& peer);

  void followerAddPeer(const PeerId& peer);
  void followerRemovePeer(const PeerId& peer);

  // First time join.
  std::atomic<bool> is_join_notified_;
  std::atomic<uint64_t> join_log_index_;
  PeerId join_request_peer_;
  void joinRaft();

  // ===============
  // Leader election
  // ===============

  std::atomic<int> election_timeout_ms_;  // A random value between 50 and 150 ms.
  static int setElectionTimeout();     // Set a random election timeout value.
  void conductElection();

  std::atomic<bool> follower_trackers_run_;
  std::atomic<uint64_t> last_vote_request_term_;
  void followerTrackerThread(const PeerId& peer, uint64_t term,
                             FollowerTracker* const my_tracker);

  // =====================
  // Log entries/revisions
  // =====================
  RaftChunkDataRamContainer* data_;
  void initChunkData(const proto::InitRequest& init_request);

  // Index will always be sequential, unique.
  // Leader will overwrite follower logs where index+term doesn't match.

  std::condition_variable new_entries_signal_;
  // Expects write lock for log_mutex to be acquired.
  uint64_t leaderAppendLogEntryLocked(
      const LogWriteAccess& log_writer,
      const std::shared_ptr<proto::RaftLogEntry>& new_entry,
      uint64_t current_term);

  // The two following methods assume write lock is acquired for log_mutex_.
  proto::AppendResponseStatus followerAppendNewEntries(
      const LogWriteAccess& log_writer,
      proto::AppendEntriesRequest* request);
  void followerCommitNewEntries(const LogWriteAccess& log_writer,
                                uint64_t request_commit_index, State state);
  inline void setAppendEntriesResponse(proto::AppendEntriesResponse* response,
                                proto::AppendResponseStatus status,
                                uint64_t current_commit_index,
                                uint64_t current_term,
                                uint64_t last_log_index,
                                uint64_t last_log_term) const;

  // Expects lock for log_mutex_to NOT have been acquired.
  void leaderCommitReplicatedEntries(uint64_t current_term);

  // All three of the following are called from leader or follower commit.
  void applySingleRevisionCommit(const std::shared_ptr<proto::RaftLogEntry>& entry);
  void chunkLockEntryCommit(const LogWriteAccess& log_writer,
                            const std::shared_ptr<proto::RaftLogEntry>& entry);
  void bulkApplyLockedRevisions(const LogWriteAccess& log_writer,
                                uint64_t lock_index, uint64_t unlock_index);

  std::condition_variable entry_replicated_signal_;
  std::condition_variable entry_committed_signal_;

  class DistributedRaftChunkLock {
   public:
    DistributedRaftChunkLock()
        : holder_(PeerId()),
          is_locked_(false),
          lock_entry_index_(0) {}
    bool writeLock(const PeerId& peer, uint64_t index) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (is_locked_) {
        // LOG(ERROR) << "Lock false for " << peer << " at log index " << index;
        return false;
      }
      CHECK(peer.isValid());
      CHECK_GT(index, 0);
      is_locked_ = true;
      holder_ = peer;
      lock_entry_index_ = index;
      return true;
    }
    bool unlock() {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!is_locked_) {
        LOG(ERROR) << "Unlock false";
        return false;
      }
      is_locked_ = false;
      lock_entry_index_ = 0;
      holder_ = PeerId();
      return true;
    }
    uint64_t lock_entry_index() const {
      std::lock_guard<std::mutex> lock(mutex_);
      return lock_entry_index_;
    }
    bool isLocked() const {
      std::lock_guard<std::mutex> lock(mutex_);
      return is_locked_;
    }
    const PeerId& holder() const {
      std::lock_guard<std::mutex> lock(mutex_);
      return holder_;
    }
    bool isLockHolder(const PeerId& peer) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!holder_.isValid() || !peer.isValid()) {
        return false;
      }
      return (holder_ == peer);
    }

   private:
    PeerId holder_;
    bool is_locked_;
    uint64_t lock_entry_index_;
    mutable std::mutex mutex_;
  };
  DistributedRaftChunkLock raft_chunk_lock_;
  // Should only be used by handleChunkLockRequest.
  std::mutex chunk_lock_mutex_;

  // Raft Chunk Requests.
  uint64_t sendChunkLockRequest(uint64_t serial_id);
  uint64_t sendChunkUnlockRequest(uint64_t serial_id, uint64_t lock_index,
                                  bool proceed_commits);
  // New revision request.
  uint64_t sendInsertRequest(const Revision::ConstPtr& item, uint64_t serial_id,
                             bool is_retry_attempt);
  bool waitAndCheckCommit(uint64_t index, uint64_t append_term,
                          uint64_t serial_id);
  bool sendLeaveRequest(uint64_t serial_id);
  void sendLeaveSuccessNotification(const PeerId& peer);

  proto::RaftChunkRequestResponse processChunkLockRequest(
      const PeerId& sender, uint64_t serial_id, bool is_retry_attempt);
  proto::RaftChunkRequestResponse processChunkUnlockRequest(
      const PeerId& sender, uint64_t serial_id, bool is_retry_attempt,
      uint64_t lock_index, uint64_t proceed_commits);
  proto::RaftChunkRequestResponse processInsertRequest(
      const PeerId& sender, uint64_t serial_id, bool is_retry_attempt,
      proto::Revision* unowned_revision_pointer);

  // ========================
  // Owner chunk information.
  // ========================
  std::string table_name_;
  common::Id chunk_id_;
  template <typename RequestType>
  void fillMetadata(RequestType* destination) const {
    CHECK_NOTNULL(destination);
    destination->mutable_metadata()->set_table(this->table_name_);
    this->chunk_id_.serialize(
        destination->mutable_metadata()->mutable_chunk_id());
  }
};

}  // namespace map_api

#endif  // MAP_API_RAFT_NODE_H_
