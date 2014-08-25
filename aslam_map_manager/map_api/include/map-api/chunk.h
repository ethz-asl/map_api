#ifndef MAP_API_CHUNK_H_
#define MAP_API_CHUNK_H_

#include <condition_variable>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>

#include <Poco/RWLock.h>

#include <zeromq_cpp/zmq.hpp>

#include "map-api/cr-table-ram-sqlite.h"
#include "map-api/id.h"
#include "map-api/peer-handler.h"
#include "map-api/message.h"
#include "map-api/revision.h"
#include "./chunk.pb.h"

namespace map_api {
/**
 * A chunk is the smallest unit of data sharing among the map_api peers. Each
 * item in a table belongs to some chunk, and each chunk contains data from only
 * one table. A chunk size should be chosen that allows reasonably fast data
 * exchange per chunk while at the same time keeping the amount of chunks to
 * be managed at a peer at a reasonable level. For
 * each chunk, a peer maintains a list of other peers holding the same chunk.
 * By holding a chunk, each peer agrees to the following contract:
 *
 * 1) It always maintains the latest version of the data contained in the chunk
 * 2) It always shares the latest version of the data with the other peers
 *    (holding the same chunk) that it is connected to.
 * 3) If any peer that is not yet a chunk holder requests any data contained in
 *    the chunk, it sends the entire chunk to that peer. That peer is then
 *    obligated to become a chunk holder as well
 * 4) It participates in providing a distributed lock for modification of the
 *    data contained in the chunk
 *
 * A consequence of 2) and 4) is that each chunk holder will be automatically
 * notified about changes in the chunk data. This allows an easy implementation
 * of triggers, as specified by Stephane, through the chunks.
 *
 * Chunk ownership may be relinquished at any time, automatically relinquishing
 * access to the latest data in the chunk and the right to modify it.
 *
 * For the time being, Chunks are NOT robust to sudden loss of connectivity -
 * this could be fixed by adapting a consensus protocol such as Raft.
 */
class Chunk {
  friend class ChunkTransaction;

 public:
  bool init(const Id& id, CRTable* underlying_table);
  bool init(const Id& id, const proto::InitRequest& request,
            const PeerId& sender, CRTable* underlying_table);

  Id id() const;

  void dumpItems(const LogicalTime& time, CRTable::RevisionMap* items);
  size_t numItems(const LogicalTime& time);
  size_t itemsSizeBytes(const LogicalTime& time);

  bool insert(Revision* item);
  bool bulkInsert(const CRTable::RevisionMap& items);

  int peerSize() const;

  void enableLockLogging();

  void leave();

  void writeLock();

  void readLock();

  bool isLocked();

  void unlock();

  /**
   * Requests all peers in MapApiHub to participate in a given chunk.
   * At the moment, this is not disputable by the other peers.
   */
  int requestParticipation();

  /**
   * Update: First locks chunk, then sends update to all peers for patching.
   * Requires underlying table to be CRU (verified).
   */
  void update(Revision* item);

  /**
   * Allows attaching a callback to incoming patch requests (insert/update).
   * The callback is passed the ID of the inserted/modified item.
   */
  void attachTrigger(const std::function<void(const Id& id)>& callback);

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
  void bulkInsertLocked(const CRTable::RevisionMap& items,
                        const LogicalTime& time);
  void updateLocked(const LogicalTime& time, Revision* item);

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
  void distributedReadLock();

  void distributedWriteLock();

  void distributedUnlock();

  template <typename RequestType>
  void fillMetadata(RequestType* destination);

  /**
   * Returns true iff lock status is WRITE_LOCKED and lock holder is self.
   * IMPORTANT: the user is responsible for locking lock_.lock
   * (unfortunately, isWriter can't lock this as it might be called from a
   * context where that lock is already acquired, and recursive_mutex isn't
   * compatible with conditional_variable)
   */
  bool isWriter(const PeerId& peer);

  void prepareInitRequest(Message* request);

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
  static void handleConnectRequestThread(Chunk* self, const PeerId& peer);
  void handleInsertRequest(const Revision& item, Message* response);
  void handleLeaveRequest(const PeerId& leaver, Message* response);
  void handleLockRequest(const PeerId& locker, Message* response);
  void handleNewPeerRequest(const PeerId& peer, const PeerId& sender,
                            Message* response);
  void handleUnlockRequest(const PeerId& locker, Message* response);
  void handleUpdateRequest(const Revision& item, const PeerId& sender,
                           Message* response);

  Id id_;
  PeerHandler peers_;
  CRTable* underlying_table_;
  DistributedRWLock lock_;
  std::function<void(const Id& id)> trigger_ = [](const Id& /*id*/) {
  };  // NOLINT
  std::mutex trigger_mutex_;
  std::mutex add_peer_mutex_;
  Poco::RWLock leave_lock_;
  bool relinquished_ = false;
  bool log_locking_ = false;
  size_t self_rank_;

  static const char kLockSequenceFile[];
  enum LockState {
    UNLOCKED,
    READ_ATTEMPT,
    READ_SUCCESS,
    WRITE_ATTEMPT,
    WRITE_SUCCESS
  };
  LockState current_state_;
  typedef std::chrono::time_point<std::chrono::system_clock> TimePoint;
  TimePoint current_state_start_;
  TimePoint global_start_;
  std::thread::id main_thread_id_;

  void startState(LockState new_state);
  void logStateDuration(LockState state, const TimePoint& start,
                        const TimePoint& end) const;
};

}  // namespace map_api

#include "map-api/chunk-inl.h"

#endif  // MAP_API_CHUNK_H_
