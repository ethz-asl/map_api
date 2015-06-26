// NOTES
//
// Lock Order
//
// integrate_mutex_
// initialize_mutex_
//
// node lock, peer lock, data lock
// node_lock, lock_monitor_mutex_
//

#ifndef MAP_API_CHORD_INDEX_H_
#define MAP_API_CHORD_INDEX_H_

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include <gtest/gtest_prod.h>
#include <multiagent-mapping-common/reader-writer-lock.h>
#include <multiagent-mapping-common/condition.h>

#include "./chord-index.pb.h"
#include "map-api/peer-id.h"

namespace map_api {

/**
 * This is the base class for all distributed indices. It implements distributed
 * key lookup via the Chord protocol, with m fixed to 16 TODO(tcies) flex?.
 * Because multiple indices will be used throughout map api, each index will
 * need to send chord RPC's in a way that the recipients will know what index
 * an RPC belongs to. Consequently, the implementation of RPCs is left to
 * derived classes. Similarly, the holder of derived classes must also ensure
 * proper routing to the handlers.
 * TODO(tcies) key responsibility & replication - will be quite a challenge
 */
class ChordIndex {
 public:
  typedef uint16_t Key;
  typedef std::unordered_map<std::string, std::string> DataMap;
  // TODO(tcies) in the long term, public functions
  // shouldn't expose these kinds of typedefs unless e.g. a serialization
  // method is given as well
  // static constexpr size_t kSuccessorListSize = 3; TODO(tcies) later
  static constexpr size_t kNumReplications = 3;
  static constexpr uint64_t kLockTimeoutMs = 10000;

  virtual ~ChordIndex();

  // ========
  // HANDLERS
  // ========
  // Return false on failure
  bool handleGetClosestPrecedingFinger(const Key& key, PeerId* result);
  bool handleGetSuccessor(PeerId* result);
  bool handleGetPredecessor(PeerId* result);
  bool handleLock(const PeerId& requester);
  bool handleUnlock(const PeerId& requester);
  bool handleNotify(const PeerId& peer_id, proto::NotifySenderType sender_type);
  bool handleReplace(const PeerId& old_peer, const PeerId& new_peer);
  bool handleAddData(const std::string& key, const std::string& value);
  bool handleRetrieveData(const std::string& key, std::string* value);
  bool handleFetchResponsibilities(
      const PeerId& requester, DataMap* responsibilities);
  bool handlePushResponsibilities(const DataMap& responsibilities);
  bool handleInitReplicator(int index, DataMap* data, const PeerId& peer);
  bool handleAppendToReplicator(int index, const DataMap& data,
                                const PeerId& peer);

  // ====================
  // HIGH-LEVEL FUNCTIONS
  // ====================
  // TODO(tcies) all/most else private/protected?
  /**
   * Adds data to index, overwrites if key exists.
   */
  bool addData(const std::string& key, const std::string& value);
  bool retrieveData(const std::string& key, std::string* value);

  static constexpr size_t kNumFingers = sizeof(Key) * 8;
  /**
   * Find successor to key, i.e. who holds the information associated with key
   * It is the first node whose hash key is larger than or equal to the key.
   */
  bool findSuccessor(const Key& key, PeerId* result);
  /**
   * First node whose hash key is strictly smaller than the key.
   */
  bool findPredecessor(const Key& key, PeerId* result);

  /**
   * Equivalent to join(nil) in the Chord paper
   */
  void create();

  bool join(const PeerId& other);
  bool cleanJoin(const PeerId& other);
  void stabilizeJoin(const PeerId& other);

  bool sendInitReplicatorRpc(const PeerId& to, int index);

  /**
   * Argument-free versions (un)lock self
   */
  bool lock();
  bool lock(const PeerId& subject);
  void unlock();
  bool unlock(const PeerId& subject);
  bool lockPeersInOrder(const PeerId& subject_1, const PeerId& subject_2);
  bool unlockPeers(const PeerId& subject_1, const PeerId& subject_2);

  bool lockPeersInArgOrder(const PeerId& subject_1, const PeerId& subject_2,
                           const PeerId& subject_3);

  void updateLastHeard(const PeerId& peer);

  /**
   * Terminates stabilizeThread();, pushes responsible data
   */
  void leave();
  void leaveClean();

  template <typename DataType>
  static Key hash(const DataType& data);

  enum class RpcStatus {
    SUCCESS,
    DECLINED,
    RPC_FAILED
  };

 protected:
  /**
   * Returns index of finger which is counter-clockwise closest to key.
   */
  PeerId closestPrecedingFinger(const Key& key);

  /**
   * Check whether key is is same as from_inclusive or between from_inclusive
   * and to_exclusive, clockwise. In particular, returns true if from_inclusive
   * is the same as to_exclusive.
   */
  static bool isIn(const Key& key, const Key& from_inclusive,
                   const Key& to_exclusive);

 private:
  friend class TestChordIndex;
  // ======================
  // REQUIRE IMPLEMENTATION
  // ======================
  // core RPCs
  virtual bool getClosestPrecedingFingerRpc(
      const PeerId& to, const Key& key, PeerId* closest_preceding) = 0;
  virtual bool getSuccessorRpc(const PeerId& to, PeerId* successor) = 0;
  virtual bool getPredecessorRpc(const PeerId& to, PeerId* predecessor) = 0;
  virtual RpcStatus lockRpc(const PeerId& to) = 0;
  virtual RpcStatus unlockRpc(const PeerId& to) = 0;
  virtual bool notifyRpc(const PeerId& to, const PeerId& subject,
                         proto::NotifySenderType sender_type) = 0;
  virtual bool replaceRpc(
      const PeerId& to, const PeerId& old_peer, const PeerId& new_peer) = 0;
  // query RPCs
  virtual bool addDataRpc(
      const PeerId& to, const std::string& key, const std::string& value) = 0;
  virtual bool retrieveDataRpc(
      const PeerId& to, const std::string& key, std::string* value) = 0;
  // TODO(tcies) indicate range of requested data? After all, predecessor
  // should be known
  virtual bool fetchResponsibilitiesRpc(
      const PeerId& to, DataMap* responsibilities) = 0;
  virtual bool pushResponsibilitiesRpc(
      const PeerId& to, const DataMap& responsibilities) = 0;
  virtual bool initReplicatorRpc(const PeerId& to, size_t index,
                                 const DataMap& data) = 0;
  virtual bool appendToReplicatorRpc(const PeerId& to, size_t index,
                                     const DataMap& data) = 0;

  // This function gets executed after data that is allocated locally (i.e. not
  // on another peer) gets updated. Derived classes can use this to implement
  // triggers on chord data.
  virtual void localUpdateCallback(const std::string& key,
                                   const std::string& old_value,
                                   const std::string& new_value);

  static void stabilizeThread(ChordIndex* self);
  static void integrateThread(ChordIndex* self);
  bool replaceDisconnectedSuccessor();
  bool joinBetweenLockedPeers(const PeerId& predecessor,
                              const PeerId& successor);
  void fixFinger(size_t finger_index);

  void fixReplicators();

  void appendDataToAllReplicators(const DataMap& data);
  void appendDataToReplicator(size_t replicator_index, const DataMap& data);
  // Not Guaranteed to always recover all data.
  void attemptDataRecovery(const Key& from);

  struct ChordPeer {
    PeerId id;
    Key key;
    ChordPeer() : id(PeerId()) {}
    explicit ChordPeer(const PeerId& _id) : id(_id), key(hash(_id)) {}
    inline bool isValid() const {
      return id.isValid();
    }
    inline void invalidate() {
      id = PeerId();
    }
  };

  /**
   * Routine common to create() and join()
   */
  void init();
  void registerPeer(const PeerId& peer, std::shared_ptr<ChordPeer>* target);
  void setFingerPeer(const PeerId& peer, size_t finger_index);

  /**
   * Returns false if chord index terminated
   */
  bool waitUntilInitialized();

  FRIEND_TEST(ChordIndexTestInitialized, fingerRetrieveLength);

  bool areFingersReady();

  bool addDataLocally(const std::string& key, const std::string& value);

  bool retrieveDataLocally(const std::string& key, std::string* value);

  bool handleNotifyClean(const PeerId& peer_id,
                         proto::NotifySenderType sender_type);
  bool handleNotifyStabilize(const PeerId& peer_id,
                             proto::NotifySenderType sender_type);
  /**
   * Assumes peers read-locked!
   */
  void handleNotifyCommon(std::shared_ptr<ChordPeer> peer,
                          proto::NotifySenderType sender_type);

  /**
   * A finger and a successor list item may point to the same peer, yet peer
   * invalidation is more efficient if centralized. Propagation of the info
   * of invalidation is lazy.
   */
  struct Finger {
    Key base_key;
    std::shared_ptr<ChordPeer> peer;
    bool is_self = false;
    bool isValid() { return (peer && peer->isValid()); }
  };
  typedef std::shared_ptr<ChordPeer> SuccessorListItem;

  /**
   * ChordPeer.id is NOT always equal to PeerId, the key. It may be invalid if
   * the peer can't be reached any more.
   * This map is there to ensure central references to peers within the chord
   * index.
   */
  typedef std::unordered_map<PeerId, std::weak_ptr<ChordPeer> > PeerMap;
  PeerMap peers_;

  Finger fingers_[kNumFingers];
  SuccessorListItem successor_;
  std::shared_ptr<ChordPeer> predecessor_;
  common::ReaderWriterMutex peer_lock_;

  FRIEND_TEST(ChordIndexTestInitialized, onePeerJoin);
  friend class ChordIndexTestInitialized;

  Key own_key_ = hash(PeerId::self());
  std::shared_ptr<ChordPeer> self_;

  bool initialized_ = false;
  FRIEND_TEST(ChordIndexTest, create);
  std::mutex initialized_mutex_;
  std::condition_variable initialized_cv_;

  bool integrated_ = false;
  std::mutex integrate_mutex_;

  std::thread stabilizer_;
  volatile bool terminate_ = false;

  // TODO(tcies) data stats: Has it already been requested?
  DataMap data_;
  common::ReaderWriterMutex data_lock_;

  // Data from other nodes replicated here.
  DataMap replicated_data_[kNumReplications];
  PeerId replicated_peers_[kNumReplications];
  std::atomic<bool> replication_ready_;
  common::Condition replication_ready_condition_;
  common::ReaderWriterMutex replicated_data_lock_;

  // Other nodes that replicate data of this node.
  PeerId replicators_[kNumReplications];
  std::mutex replicator_peer_mutex_;

  std::mutex node_lock_;
  bool node_locked_ = false;
  PeerId node_lock_holder_;
  std::chrono::time_point<std::chrono::system_clock> last_heard_;
  std::thread lock_motitor_thread_;
  std::mutex lock_monitor_mutex_;
  std::atomic<bool> lock_motitor_thread_running_;
  void lockMonitor();
};

}  // namespace map_api
#include "./chord-index-inl.h"
#endif  // MAP_API_CHORD_INDEX_H_
