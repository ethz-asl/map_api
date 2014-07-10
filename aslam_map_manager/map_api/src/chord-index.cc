#include "map-api/chord-index.h"

#include <type_traits>

#include <glog/logging.h>

#include <Poco/DigestStream.h>
#include <Poco/MD5Engine.h>

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
  CHECK(false) << "Locking not implemented";
  // Cases successor or predecessor leaves
  // TODO(tcies) hooks for derived classes: need to move around data!
  if (leaver == successor_.second) {
    successor_.second = leaver_successor;
  }
  if (leaver == predecessor_.second) {
    predecessor_.first = hash(leaver_predecessor);
    predecessor_.second = leaver_predecessor;
  }
  // We might need to update our fingers
  for (size_t i = 0; i < M; ++i) {  // finger[0] is successor_
    if (fingers_[i].second == leaver) {
      fingers_[i].second = leaver_successor;
      // in a sparsely populated chord ring, multiple fingers can point to the
      // same peer, so no break intended.
    }
  }
  return true;
}

bool ChordIndex::handleNotifySuccessor(const PeerId& predecessor) {
  predecessor_ = std::make_pair(hash(predecessor), predecessor);
  // TODO(tcies) push data to newly joined peer
  return true;
}

bool ChordIndex::handleNotifyPredecessor(const PeerId& successor) {
  successor_.second = successor;
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
  init();
  for (size_t i = 0; i < M; ++i) {
    fingers_[i].second = PeerId::self();
  }
  predecessor_ = std::make_pair(own_key_, PeerId::self());
  initialized_ = true;
}

void ChordIndex::join(const PeerId& other) {
  init();
  for (size_t i = 0; i < M; ++i) {
    PeerId finger = findSuccessorRpc(other, fingers_[i].first);
    fingers_[i].second = finger;
  }
  PeerId predecessor = getPredecessorRpc(successor_.second);
  Key predecessor_key = hash(predecessor);
  CHECK(predecessor_key != own_key_);
  predecessor_ = std::make_pair(predecessor_key, predecessor);

  initialized_ = true;
  notifyPredecessorRpc(predecessor_.second, PeerId::self());
  notifySuccessorRpc(successor_.second, PeerId::self());
}

void ChordIndex::leave() {
  leaving_ = true;
  leaveRpc(successor_.second, PeerId::self(), predecessor_.second,
           successor_.second);
  // TODO(tcies) move data to successor
  initialized_ = false;
}

int ChordIndex::closestPrecedingFinger(const Key& key) const {
  // TODO(tcies) verify corner cases
  CHECK(false) << "Corner cases not verified";
  for (size_t i = 0; i < M; ++i) {
    size_t index = M - 1 - i;
    Key actual_key = hash(fingers_[index].second);
    if (isIn(actual_key, own_key_, key)) {
      return index;
    }
  }
  LOG(FATAL) << "Called closest preceding finger on key which is smaller " <<
      "than successor key";
}

PeerId ChordIndex::findSuccessorAndFixFinger(
    int finger_index, const Key& query) {
  PeerId better_finger_node, response;
  response = findSuccessorAndFixFingerRpc(
      fingers_[finger_index].second, query, fingers_[finger_index].first,
      &better_finger_node);
  fingers_[finger_index].second = better_finger_node;
  return response;
}

ChordIndex::Key ChordIndex::hash(const PeerId& id) const {
  // TODO(tcies) better method?
  Poco::MD5Engine md5;
  Poco::DigestOutputStream digest_stream(md5);
  digest_stream << id;
  digest_stream.flush();
  const Poco::DigestEngine::Digest& digest = md5.digest();
  bool diges_still_uchar_vec =
      std::is_same<
      Poco::DigestEngine::Digest, std::vector<unsigned char> >::value;
  CHECK(diges_still_uchar_vec) <<
      "Underlying type of Digest changed since Poco 1.3.6";
  union KeyUnion {
    Key key;
    unsigned char bytes[sizeof(Key)];
  };
  CHECK_EQ(sizeof(Key), sizeof(KeyUnion));
  KeyUnion return_value;
  for (size_t i = 0; i < sizeof(Key); ++i) {
    return_value.bytes[i] = digest[i];
  }
  return return_value.key;
}

void ChordIndex::init() {
  own_key_ = hash(PeerId::self());
  for (size_t i = 0; i < M; ++i) {
    fingers_[i].first = own_key_ + (1 << i); // overflow intended
  }
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
