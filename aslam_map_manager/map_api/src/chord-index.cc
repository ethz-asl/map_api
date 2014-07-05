#include "map-api/chord-index.h"

#include <glog/logging.h>

namespace map_api {

PeerId ChordIndex::handleFindSuccessor(const Key& key) {
  CHECK(initialized_);
  return findSuccessor(key);
}

PeerId ChordIndex::handleGetPredecessor() {
  CHECK(initialized_);
  return predecessor_.second;
}

PeerId ChordIndex::handleFindSuccessorAndFixFinger(
    const Key& query, const Key& finger_base, PeerId* actual_finger_node) {
  CHECK(initialized_);
  CHECK_NOTNULL(actual_finger_node);
  if (isIn(predecessor_.first, finger_base, own_key_)) {
    *actual_finger_node = predecessor_.second;
  } else {
    *actual_finger_node = PeerId::self();
  }
  return findSuccessor(query);
}

bool ChordIndex::handleLeave(
    const PeerId& leaver, const PeerId&leaver_predecessor,
    const PeerId& leaver_successor) {
  CHECK(initialized_);
  CHECK(leaver != leaver_predecessor);
  CHECK(leaver != leaver_successor);
  // Case the request originated here
  if (leaver == PeerId::self()) {
    CHECK(leaving_);
    CHECK(leaver_successor == successor_.second);
    CHECK(leaver_predecessor == predecessor_.second);
    return true;
  }
  // forward rpc while successor might still be leaver
  CHECK(leaveRpc(successor_.second, leaver, leaver_predecessor,
                 leaver_successor));
  // TODO(tcies) locking
  CHECK(false);
  // Cases successor or predecessor leaves
  if (leaver == successor_.second) {
    successor_.second = leaver_successor;
  }
  if (leaver == predecessor_.second) {
    predecessor_.first = hash(leaver_predecessor);
    predecessor_.second = leaver_predecessor;
  }
  // We might need to update our fingers
  for (size_t i = 0; i < M; ++i) { // finger[0] is successor_
    if (fingers_[i].second == leaver) {
      fingers_[i].second = leaver_successor;
    }
  }
  return true;
}

PeerId ChordIndex::findSuccessor(const Key& key) {
  if (isIn(key, own_key_, fingers_[0].first)) {
    return fingers_[0].second;
  } else {
    int to_ask = closestPrecedingFinger(key);
    return findSuccessorAndFixFinger(to_ask, key);
  }
}

void ChordIndex::create() {
  predecessor_.second = successor_.second = PeerId::self();
  predecessor_.first = hash(PeerId::self());
}

void ChordIndex::join(const PeerId& other) {
  // TODO(tcies) implement
  CHECK(false);
}

void ChordIndex::leave() {
  // TODO(tcies) implement
    CHECK(false);
}

int ChordIndex::closestPrecedingFinger(const Key& key) const {
  // TODO(tcies) implement
    CHECK(false);
}

PeerId ChordIndex::findSuccessorAndFixFinger(
    int finger_index, const Key& query) {
  // TODO(tcies) implement
    CHECK(false);
}

ChordIndex::Key ChordIndex::hash(PeerId) const {
  // TODO(tcies) implement
    CHECK(false);
}

bool ChordIndex::isIn(
    const Key& key, const Key& from_inclusive, const Key& to_exclusive) const {
  if (key == from_inclusive) {
    return true;
  }
  if (from_inclusive <= to_exclusive) { // case doesn't pass 0
    return (from_inclusive < key && key < to_exclusive);
  } else { // case passes 0
    return (from_inclusive < key || key < to_exclusive);
  }
}

} /* namespace map_api */
