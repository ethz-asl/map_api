#ifndef MAP_API_RAFT_CHUNK_H_
#define MAP_API_RAFT_CHUNK_H_

#include <mutex>
#include <set>

#include <multiagent-mapping-common/unique-id.h>

#include "./chunk.pb.h"
#include "map-api/chunk-base.h"

namespace map_api {
class Message;
class RaftNode;

class RaftChunk : public ChunkBase {
 public:
  virtual ~RaftChunk();

  bool init(const common::Id& id, std::shared_ptr<TableDescriptor> descriptor,
            bool initialize);
  virtual void initializeNewImpl(
      const common::Id& id,
      const std::shared_ptr<TableDescriptor>& descriptor) override;
  bool init(const common::Id& id, const proto::InitRequest& request,
            const PeerId& sender, std::shared_ptr<TableDescriptor> descriptor);

  virtual void dumpItems(const LogicalTime& time, ConstRevisionMap* items) const
      override;
  virtual size_t numItems(const LogicalTime& time) const override;
  virtual size_t itemsSizeBytes(const LogicalTime& time) const override;

  virtual void getCommitTimes(const LogicalTime& sample_time,
                              std::set<LogicalTime>* commit_times) const
      override;

  virtual bool insert(const LogicalTime& time,
                      const std::shared_ptr<Revision>& item) override;

  virtual int peerSize() const override;

  // May not need these???
  // Non-const intended to avoid accidental write-lock while reading.
  virtual void writeLock() override;

  virtual void readLock() const override;

  virtual bool isWriteLocked() override;

  virtual void unlock() const override;

  /**
   * Requests all peers in MapApiHub to participate in a given chunk.
   * At the moment, this is not disputable by the other peers.
   */

  // May need to remove these..
  virtual int requestParticipation() override;
  virtual int requestParticipation(const PeerId& peer) override;

  /**
   * Update: First locks chunk, then sends update to all peers for patching.
   * Requires underlying table to be CRU (verified).
   */
  virtual void update(const std::shared_ptr<Revision>& item) override;

  virtual LogicalTime getLatestCommitTime() const override;

  static const char kConnectRequest[];
  static const char kInitRequest[];
  static const char kInsertRequest[];
  static const char kLeaveRequest[];
  static const char kLockRequest[];
  static const char kNewPeerRequest[];
  static const char kUnlockRequest[];
  static const char kUpdateRequest[];

 private:
  /**
   * insert and update for transactions.
   */
  virtual void bulkInsertLocked(const MutableRevisionMap& items,
                                const LogicalTime& time) override;
  virtual void updateLocked(const LogicalTime& time,
                            const std::shared_ptr<Revision>& item) override;
  virtual void removeLocked(const LogicalTime& time,
                            const std::shared_ptr<Revision>& item) override;

  /**
   * Adds a peer to the chunk swarm by sending it an init request. Assumes
   * lock_ is write-locked. I.e., this function is intended to be called from
   * handleConnectRequest() and requestParticipation().
   * This function MAY NOT be executed in parallel  for multiple peers, as each
   * new peer must be immediately informed about the addresses of the full
   * swarm. This is enforced by the add_peer_mutex.
   * Also, while this function verifies that the chunk is locked at the
   * beginning of execution, another thread MAY NOT unlock the chunk. This is
   * enforced by having distributedUnlock() lock add_peer_mutex_.
   * Finally, the peer MAY NOT be already in the swarm. Functions calling this
   * function should check for that themselves if it is OK by them.
   * The function returns false iff the peer is not in the swarm but refuses
   * to join it by responding with Message::kDecline.
   */
  bool addPeer(const PeerId& peer);
  size_t addAllPeers();
  /**
   * Distributed RW lock structure. Because it is distributed, unlocking from
   * a remote peer can potentially be handled by a different thread than the
   * locking one - thus an extra layer of lock is needed. The lock state is
   * represented by an enum variable.
   */
  struct DistributedRWLock {
    enum class State {
      UNLOCKED,
      READ_LOCKED,
      ATTEMPTING,
      WRITE_LOCKED
    };
    State state = State::UNLOCKED;
    State preempted_state = State::UNLOCKED;
    int n_readers = 0;
    PeerId holder;
    std::thread::id thread;
    int write_recursion_depth = 0;  // the write lock is recursive
    // to avoid deadlocks, this mutex may not be locked while awaiting replies
    std::mutex mutex;
    std::condition_variable cv;  // in case writeLock can't be acquired
    DistributedRWLock() {}
  };
  /**
   * The holder may acquire a read lock without the need to communicate with
   * the other peers - a read lock manifests itself only in that the holder
   * defers distributed write lock requests until unlocking or denies them
   * altogether.
   */
  void distributedReadLock() const;

  // Non-const intended to avoid accidental write-lock while reading.
  void distributedWriteLock();

  void distributedUnlock() const;

  template <typename RequestType>
  void fillMetadata(RequestType* destination) const;

  /**
   * Returns true iff lock status is WRITE_LOCKED and lock holder is self.
   * IMPORTANT: the user is responsible for locking lock_.lock
   * (unfortunately, isWriter can't lock this as it might be called from a
   * context where that lock is already acquired, and recursive_mutex isn't
   * compatible with conditional_variable)
   */
  bool isWriter(const PeerId& peer) const;

  void initRequestSetData(proto::InitRequest* request);
  void initRequestSetPeers(proto::InitRequest* request);
  void prepareInitRequest(Message* request);

  inline void syncLatestCommitTime(const Revision& item);

  /**
   * ====================================================================
   * Handlers for ChunkManager requests that are addressed at this Chunk.
   * ====================================================================
   */
  friend class NetTable;
  /**
   * Handles insert requests
   */
  void handleConnectRequest(const PeerId& peer, Message* response);
  static void handleConnectRequestThread(LegacyChunk* self, const PeerId& peer);
  void handleInsertRequest(const std::shared_ptr<Revision>& item,
                           Message* response);
  void handleLeaveRequest(const PeerId& leaver, Message* response);
  void handleLockRequest(const PeerId& locker, Message* response);
  void handleNewPeerRequest(const PeerId& peer, const PeerId& sender,
                            Message* response);
  void handleUnlockRequest(const PeerId& locker, Message* response);
  void handleUpdateRequest(const std::shared_ptr<Revision>& item,
                           const PeerId& sender, Message* response);

  void awaitInitialized() const;

  virtual void leaveImpl() override;
  virtual void awaitShared() override;

  PeerHandler peers_;
  mutable DistributedRWLock lock_;
  mutable std::mutex add_peer_mutex_;
  ReaderWriterMutex leave_lock_;
  volatile bool initialized_ = false;
  volatile bool relinquished_ = false;
  bool log_locking_ = false;
  size_t self_rank_;
  LogicalTime latest_commit_time_;

  static const char kLockSequenceFile[];
  enum LockState {
    UNLOCKED,
    READ_ATTEMPT,
    READ_SUCCESS,
    WRITE_ATTEMPT,
    WRITE_SUCCESS
  };
  mutable LockState current_state_;
  typedef std::chrono::time_point<std::chrono::system_clock> TimePoint;
  mutable TimePoint current_state_start_;
  TimePoint global_start_;
  std::thread::id main_thread_id_;

  void startState(LockState new_state) const;
  void logStateDuration(LockState state, const TimePoint& start,
                        const TimePoint& end) const;
};

}  // namespace map_api

#endif  // MAP_API_RAFT_CHUNK_H_
