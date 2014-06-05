#ifndef CHUNK_H
#define CHUNK_H

#include <memory>
#include <string>
#include <set>

#include <zeromq_cpp/zmq.hpp>

#include "map-api/peer.h"
#include "map-api/message.h"
#include "map-api/revision.h"

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
   * the network. If a peer a wants to access data it does not posess, it
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
  bool init();
  /**
   * Insert new item into this chunk: Item gets sent to all peers
   */
  bool insert(const Revision& item);
  /**
   * The holder may acquire a read lock without the need to communicate with
   * the other peers - a read lock manifests itself only in that the holder
   * defers distributed write lock requests until unlocking or denies them
   * altogether.
   */
  void readLock();
  /**
   * Acquiring write locks happens over the network: A spanning tree among the
   * peers is created, where each peer connects with all other peers that are
   * known to it that aren't yet in the spanning tree. The locking request is
   * propagated from root to leaves (down the tree) while the lock is granted
   * up the tree. The following responses to a lock request are possible:
   * - AM_READING, alternatively the request could also be blocked until
   *               the read lock is released
   * - HAVE_SEEN_THIS_REQUEST ensuring that the tree remains acyclic. This ends
   *               lock-related communication with the corresponding peer.
   * - GRANTED the lock is granted recursively: A node responds with GRANTED
   *               if all the peers it has contacted have responded with
   *               GRANTED or HAVE_SEEN_THIS_REQUEST (upward propagation)
   * - CONFLICT if the peer maintains another lock holder or lock requester
   *
   * It yet needs to be specified what to do in the general case when a conflict
   * is returned. I suggest to assume full connectedness in the first version.
   * This will lead to a star topology instead of a tree topology, allowing to
   * use majority count for conflict resolution: In case of conflict, each
   * "locker" calculates the ratio of #GRANTED/#CONFLICT. If it is > 1, it
   * assumes it has acquired the lock - if it is exactly 1, the "locking" peer
   * with the lexicographically lower socket identification takes the lock.
   *
   * To be robust against loss of connectivity, each request should have a
   * timeout that uses the synchronized clock.
   */
  void writeLock();
  /**
   * Unlocking a lock should be coupled to sending the updated data TODO(tcies)
   * This would ensure that all peers can satisfy 1) and 2) of the
   * aforementioned contract.
   */
  void unlock();

  static void handleConnectRequest(const std::string& serialized_request,
                                   Message* response);

  static const char kInsertRequest[];
  static void handleInsertRequest(const std::string& serialized_request,
                                  Message* response);

  static void handleLockRequest(const std::string& serialized_request,
                                Message* response);

  static void handleUnlockRequest(const std::string& serialized_request,
                                  Message* response);

  /**
   * Propagates removal of peers from the network.
   */
  static void handleRelinquishNotification(
      const std::string& serialized_notification);

 private:
  /**
   * Weak pointer because a list of all peers that are connected in map_api is
   * expected to be maintained centrally at MapApiHub.
   */
  std::set<std::weak_ptr<Peer> > peers_;

  enum LockStatus {
    UNLOCKED,
    READ_LOCKED,
    WRITE_LOCK_REQUESTED,
    WRITE_LOCKED
  };
  LockStatus lock_status_;
  std::weak_ptr<Peer> lock_holder_;

};

} //namespace map_api
#endif // CHUNK_H
