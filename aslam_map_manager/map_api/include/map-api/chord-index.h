#ifndef MAP_API_CHORD_INDEX_H_
#define MAP_API_CHORD_INDEX_H_

#include <memory>
#include <mutex>
#include <unordered_map>

#include "map-api/message.h"
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
  typedef uint16_t Key; //TODO(tcies) in the long term, public functions
  // shouldn't expose these kinds of typedefs unless e.g. a serialization
  // method is given as well
  // static constexpr size_t kSuccessorListSize = 3; TODO(tcies) later

  virtual ~ChordIndex();

  // ========
  // HANDLERS
  // ========
  PeerId handleFindSuccessor(const Key& key);
  PeerId handleGetPredecessor();
  /**
   * Any peer notifies us about their existence.
   */
  void handleNotify(const PeerId& peer_id);

  static constexpr size_t M = sizeof(Key) * 8;
  /**
   * Find successor to key, i.e. who holds the information associated with key
   * It is the first node whose hash key is larger than or equal to the key.
   */
  PeerId findSuccessor(const Key& key);

  /**
   * Equivalent to join(nil) in the Chord paper
   */
  void create();

  void join(const PeerId& other);
  /**
   * Terminates stabilizeThread();
   */
  void leave();

  /**
   * Generates hash from PeerId.
   */
  static Key hash(const PeerId& id);

 private:
  // ======================
  // REQUIRE IMPLEMENTATION
  // ======================
  virtual bool findSuccessorRpc(const PeerId& to, const Key& argument,
                                PeerId* successor) = 0;
  virtual bool getPredecessorRpc(const PeerId& to, PeerId* predecessor) = 0;
  virtual bool notifyRpc(const PeerId& to, const PeerId& self) = 0;

  void stabilizeThread();

  struct ChordPeer {
    PeerId id;
    Key key;
    ChordPeer(const PeerId& _id) : id(_id), key(hash(_id)) {}
    inline bool isValid() {
      return id.isValid();
    }
    inline void invalidate() {
      id = PeerId();
    }
  };

  /**
   * Returns index of finger which is counter-clockwise closest to key.
   */
  std::shared_ptr<ChordIndex::ChordPeer> closestPrecedingFinger(
      const Key& key) const;
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
  bool isIn(const Key& key, const Key& from_inclusive,
            const Key& to_exclusive) const;

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

  std::mutex peer_access_;

  Key own_key_ = hash(PeerId::self());
  std::shared_ptr<ChordPeer> self_;

  bool initialized_ = false;
  bool terminate_ = false;
};

} /* namespace map_api */

#endif /* MAP_API_CHORD_INDEX_H_ */
