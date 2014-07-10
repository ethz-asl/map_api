#ifndef MAP_API_CHORD_INDEX_H_
#define MAP_API_CHORD_INDEX_H_

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
 * Finally, the first implementation assumes no sporadic loss of connectivity.
 * Consequently, the robustness functions and maintenance tasks (stabilize,
 * notify, fix_fingers, check_predecessor) of chord are left out and replaced
 * with simpler mechanisms (findSuccessorAndFixFinger(), leave()).
 *
 * TODO(tcies) locking, decline messages & handling.
 */
class ChordIndex {
 public:
  typedef uint16_t Key; //TODO(tcies) in the long term, public functions
  // shouldn't expose these kinds of typedefs unless e.g. a serialization
  // method is given as well

  virtual ~ChordIndex();

  // ========
  // HANDLERS
  // ========
  PeerId handleFindSuccessor(const Key& key);
  PeerId handleGetPredecessor();
  PeerId handleFindSuccessorAndFixFinger(
      const Key& query, const Key& finger_base, PeerId* actual_finger_node);
  bool handleLeave(const PeerId& leaver, const PeerId&leaver_predecessor,
                   const PeerId& leaver_successor);
  bool handleNotifySuccessor(const PeerId& predecessor);
  bool handleNotifyPredecessor(const PeerId& successor);

  static constexpr size_t M = sizeof(Key) * 8;
  /**
   * Find successor to key, i.e. who holds the information associated with key
   */
  PeerId findSuccessor(const Key& key);

  void create();

  /**
   * Differs from chord in that the successor will directly inform its
   * predecessor about the newly joined node, and will inform the newly joined
   * node about its predecessor.
   */
  void join(const PeerId& other);

  /**
   * Differs from chord in that a leave message is sent around the circle,
   * such that all nodes can remove bad links from their fingers directly.
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
  virtual PeerId findSuccessorRpc(const PeerId& to, const Key& argument) = 0;
  virtual PeerId getPredecessorRpc(const PeerId& to) = 0;
  virtual PeerId findSuccessorAndFixFingerRpc(
      const PeerId& to, const Key& query, const Key& finger_base,
      PeerId* actual_finger_node) = 0;
  virtual bool leaveRpc(
      const PeerId& to, const PeerId& leaver, const PeerId&leaver_predecessor,
      const PeerId& leaver_successor) = 0;
  virtual bool notifySuccessorRpc(
      const PeerId& successor, const PeerId& self) = 0;
  virtual bool notifyPredecessorRpc(
      const PeerId& predecessor, const PeerId& self) = 0;

  /**
   * Returns index of finger which is counter-clockwise closest to key.
   */
  int closestPrecedingFinger(const Key& key) const;
  /**
   * Slight departure from original chord protocol, linked to assumption of no
   * loss of connectivity: findSuccessor and finger fixing in one RPC.
   * Same as querying a finger with findSuccessor, but also making sure that
   * the reference to that link is proper according to the chord protocol.
   */
  PeerId findSuccessorAndFixFinger(int finger_index, const Key& query);
  /**
   * Routine common to create() and join()
   */
  void init();
  /**
   * Check whether key is is same as from_inclusive or between from_inclusive
   * and to_exclusive
   */
  bool isIn(const Key& key, const Key& from_inclusive,
            const Key& to_exclusive) const;


  bool initialized_ = false;
  bool leaving_ = false;
  std::pair<Key, PeerId> fingers_[M];
  std::pair<Key, PeerId>& successor_ = fingers_[0];
  std::pair<Key, PeerId> predecessor_;
  Key own_key_ = hash(PeerId::self());
};

} /* namespace map_api */

#endif /* MAP_API_CHORD_INDEX_H_ */
