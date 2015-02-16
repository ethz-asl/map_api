#ifndef MAP_API_CHORD_INDEX_H_
#define MAP_API_CHORD_INDEX_H_

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include <gtest/gtest_prod.h>

#include "map-api/peer-id.h"
#include "map-api/reader-writer-lock.h"

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
  bool handleNotify(const PeerId& peer_id);
  bool handleReplace(const PeerId& old_peer, const PeerId& new_peer);
  bool handleAddData(const std::string& key, const std::string& value);
  bool handleRetrieveData(const std::string& key, std::string* value);
  bool handleFetchResponsibilities(
      const PeerId& requester, DataMap* responsibilities);
  bool handlePushResponsibilities(const DataMap& responsibilities);

  // ====================
  // HIGH-LEVEL FUNCTIONS
  // ====================
  // TODO(tcies) all/most else private/protected?
  /**
   * Adds data to index, overwrites if key exists.
   */
  bool addData(const std::string& key, const std::string& value);
  bool retrieveData(const std::string& key, std::string* value);

  static constexpr size_t M = sizeof(Key) * 8;
  /**
   * Find successor to key, i.e. who holds the information associated with key
   * It is the first node whose hash key is larger than or equal to the key.
   */
  PeerId findSuccessor(const Key& key);
  /**
   * First node whose hash key is strictly smaller than the key.
   */
  PeerId findPredecessor(const Key& key);

  /**
   * Equivalent to join(nil) in the Chord paper
   */
  void create();

  void join(const PeerId& other);
  void cleanJoin(const PeerId& other);
  void stabilizeJoin(const PeerId& other);

  /**
   * Argument-free versions (un)lock self
   */
  bool lock();
  bool lock(const PeerId& subject);
  void unlock();
  void unlock(const PeerId& subject);

  /**
   * Terminates stabilizeThread();, pushes responsible data
   */
  void leave();
  void leaveClean();

  template <typename DataType>
  static Key hash(const DataType& data);

 private:
  // ======================
  // REQUIRE IMPLEMENTATION
  // ======================
  // core RPCs
  virtual bool getClosestPrecedingFingerRpc(
      const PeerId& to, const Key& key, PeerId* closest_preceding) = 0;
  virtual bool getSuccessorRpc(const PeerId& to, PeerId* successor) = 0;
  virtual bool getPredecessorRpc(const PeerId& to, PeerId* predecessor) = 0;
  virtual bool lockRpc(const PeerId& to) = 0;
  virtual bool unlockRpc(const PeerId& to) = 0;
  virtual bool notifyRpc(const PeerId& to, const PeerId& subject) = 0;
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

  static void stabilizeThread(ChordIndex* self);
  static void integrateThread(ChordIndex* self);

  struct ChordPeer {
    PeerId id;
    Key key;
    explicit ChordPeer(const PeerId& _id) : id(_id), key(hash(_id)) {}
    inline bool isValid() const {
      return id.isValid();
    }
    inline void invalidate() {
      id = PeerId();
    }
  };

  /**
   * Returns index of finger which is counter-clockwise closest to key.
   */
  PeerId closestPrecedingFinger(
      const Key& key);
  /**
   * Routine common to create() and join()
   */
  void init();
  void registerPeer(const PeerId& peer, std::shared_ptr<ChordPeer>* target);

  /**
   * Check whether key is is same as from_inclusive or between from_inclusive
   * and to_exclusive, clockwise. In particular, returns true if from_inclusive
   * is the same as to_exclusive.
   */
  static bool isIn(const Key& key, const Key& from_inclusive,
                   const Key& to_exclusive);

  /**
   * Returns false if chord index terminated
   */
  bool waitUntilInitialized();

  bool addDataLocally(const std::string& key, const std::string& value);

  bool retrieveDataLocally(const std::string& key, std::string* value);

  bool handleNotifyClean(const PeerId& peer_id);
  bool handleNotifyStabilize(const PeerId& peer_id);
  /**
   * Assumes peers read-locked!
   */
  void handleNotifyCommon(std::shared_ptr<ChordPeer> peer);

  /**
   * A finger and a successor list item may point to the same peer, yet peer
   * invalidation is more efficient if centralized. Propagation of the info
   * of invalidation is lazy.
   */
  struct Finger {
    Key base_key;
    std::shared_ptr<ChordPeer> peer;
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

  Finger fingers_[M];
  SuccessorListItem successor_;
  std::shared_ptr<ChordPeer> predecessor_;

  ReaderWriterMutex peer_lock_;

  FRIEND_TEST(ChordIndexTestInitialized, onePeerJoin);
  friend class ChordIndexTestInitialized;

  std::mutex peer_access_;

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
  ReaderWriterMutex data_lock_;

  std::mutex node_lock_;
  bool node_locked_ = false;
  PeerId node_lock_holder_;
};

}  // namespace map_api
#include "./chord-index-inl.h"
#endif  // MAP_API_CHORD_INDEX_H_
