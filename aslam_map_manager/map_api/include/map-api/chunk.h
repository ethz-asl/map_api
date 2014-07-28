#ifndef CHUNK_H
#define CHUNK_H

#include <condition_variable>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>

#include <Poco/RWLock.h>

#include <zeromq_cpp/zmq.hpp>

#include "map-api/chunk-transaction.h"
#include "map-api/cr-table-ram-cache.h"
#include "map-api/id.h"
#include "map-api/peer-handler.h"
#include "map-api/message.h"
#include "map-api/revision.h"
#include "chunk.pb.h"

namespace map_api{
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
 * access to the latest data in the chunk and the right to modify it. Still,
 * the chunk data can be kept in the database and read "offline".
 *
 * A mechanism to ensure robustness of data availability against sporadic
 * relinquishing of chunk ownership among the peers is yet to be specified
 * TODO(tcies). It may consist of requests to random peers to become chunk
 * holders and/or a preferred sharing ratio.
 *
 * TODO(tcies) will need a central place to keep track of all (active) chunks -
 * to ensure uniqueness and maybe to enable automatic management. Maybe
 * MapApiHub
 */
class Chunk {
 public:
  /**
   * NB it's easier to start reading the comments on other functions.
   *
   * A chunk is typically initialized as a consequence of data lookup across
   * the network. If a peer a wants to access data it does not possess, it
   * requests all other peers it is connected to through map_api (alternatively:
   * all other peers that hold chunks in the table that the data is looked up
   * in). If one of those peers, b, has the data it looks for, b sends a the
   * data of the entire chunk and its peer list while it adds a to its own peer
   * list and shares the news about a joining with its peers.
   *
   * Peer addition is subject to synchronization as well, at least in the first
   * implementation that assumes full connectedness among peers (see writeLock()
   * comments). b needs to perform a lock with its peers just at it would for
   * modifying chunk data.
   */
  bool init(const Id& id, CRTable* underlying_table);
  bool init(const Id& id, const proto::InitRequest& request,
            const PeerId& sender, CRTable* underlying_table);

  bool check(const ChunkTransaction& transaction);

  bool commit(const ChunkTransaction& transaction);

  /**
   * Returns own identification
   */
  Id id() const;
  /**
   * Insert new item into this chunk: Item gets sent to all peers
   */
  bool insert(Revision* item);

  std::shared_ptr<ChunkTransaction> newTransaction();
  std::shared_ptr<ChunkTransaction> newTransaction(const LogicalTime& time);

  int peerSize() const;

  void leave();

  void lock();

  /**
   * Requests all peers in MapApiHub to participate in a given chunk.
   * This write-locks the chunk and directly sends init requests to the affected
   * peers. Those that respond with ACK are added to the swarm and the chunk
   * is unlocked.
   */
  int requestParticipation();

  void unlock();

  /**
   * Update: First locks chunk, then sends update to all peers for patching.
   * Requires underlying table to be CRU (verified).
   */
  void update(Revision* item);

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
    int write_recursion_depth = 0; // the write lock is recursive
    // to avoid deadlocks, this mutex may not be locked while awaiting replies
    std::mutex mutex;
    std::condition_variable cv; // in case lock can't be acquired
    DistributedRWLock() {}
  };
  /**
   * The holder may acquire a read lock without the need to communicate with
   * the other peers - a read lock manifests itself only in that the holder
   * defers distributed write lock requests until unlocking or denies them
   * altogether.
   */
  void distributedReadLock();
  /**
   * Acquiring write locks happens over the network: Unless the caller knows
   * that the lock is held by some other peer, a lock request is broadcast to
   * the chunk swarm, and the peers reply with a lock response which contains
   * the address of the peer they consider the lock holder, or either
   * acknowledge or decline, depending on the used strategy.
   *
   * SERIAL LOCK STRATEGY (the one used now, for simplicity):
   * We know the chunk swarm is fully connected, and assume the broadcast is
   * performed serially, in lexicographical order of peer addresses.
   * Then, we can either stop the broadcast when we receive a negative response
   * from the peer with the lowest address, or, once we pass this first burden,
   * may assume that all other peers will respond positively, as no other peer
   * could have gotten to them (as they would have needed to lock the first
   * peer as well). Consequently, the lock must be released in reverse
   * lexicographical order.
   *
   * PARALLEL LOCK STRATEGY (probably faster with many peers and little lock
   * contention):
   * Peers are requested in parallel and respond with the address of the peer
   * they consider lock holder.
   * If all peers respond with the address of the caller, the caller considers
   * the lock acquired.
   * In all other cases, at least one other peer is also attempting to get the
   * lock and will respond with an invalid ("") address. TODO(tcies) what if
   * disconnected? Depending on the responses of the remaining peers:
   * - If more of them have returned the address of the other peer, the caller
   * sends a lock redirect request asking the peers accepting the caller as
   * lock holder to yield the lock to the other peer. It then also yields to
   * the other peer with lock yield request.
   * - If more of them have returned the caller address, the caller waits for
   * the remaining peers to yield.
   * - If the votes are split equally, the lock contender with the lower
   * IP:port string yields.
   * Unlocking is tricky.
   *
   * TODO(tcies) benchmark serial VS parallel lock strategy?
   * TODO(tcies) define timeout after which the lock is released automatically
   * TODO(tcies) option to renew lock if operations take a long time
   */
  void distributedWriteLock();

  /**
   * Unlocking a lock should be coupled to sending the updated data TODO(tcies)
   * This would ensure that all peers can satisfy 1) and 2) of the
   * aforementioned contract.
   */
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
   * ===================================================================
   * Handles for ChunkManager requests that are addressed at this Chunk.
   * ===================================================================
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
  std::mutex add_peer_mutex_;
  Poco::RWLock leave_lock_;
  bool relinquished_ = false;
};

} //namespace map_api

#include "map-api/chunk-inl.h"

#endif // CHUNK_H
