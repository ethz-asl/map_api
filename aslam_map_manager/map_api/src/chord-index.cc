#include "map-api/chord-index.h"

#include <type_traits>

#include <glog/logging.h>
#include <gflags/gflags.h>

#include <Poco/DigestStream.h>
#include <Poco/MD5Engine.h>

const std::string kCleanJoin("clean");
const std::string kStabilizeJoin("stabilize");

DEFINE_string(join_mode, kCleanJoin,
              ("Can be " + kCleanJoin + " or " + kStabilizeJoin).c_str());
DEFINE_uint64(stabilize_us, 1000, "Interval of stabilization in microseconds");

namespace map_api {

ChordIndex::~ChordIndex() {}

template<typename DataType>
ChordIndex::Key ChordIndex::hash(const DataType& data) {
  // TODO(tcies) better method?
  Poco::MD5Engine md5;
  Poco::DigestOutputStream digest_stream(md5);
  digest_stream << data;
  digest_stream.flush();
  const Poco::DigestEngine::Digest& digest = md5.digest();
  bool digest_still_uchar_vec =
      std::is_same<
      Poco::DigestEngine::Digest, std::vector<unsigned char> >::value;
  CHECK(digest_still_uchar_vec) <<
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

bool ChordIndex::handleGetClosestPrecedingFinger(
    const Key& key, PeerId* result) {
  CHECK_NOTNULL(result);
  if (!waitUntilInitialized()) {
    return false;
  }
  *result = closestPrecedingFinger(key);
  return true;
}

bool ChordIndex::handleGetSuccessor(PeerId* result) {
  CHECK_NOTNULL(result);
  if (!waitUntilInitialized()) {
    return false;
  }
  peer_lock_.readLock();
  *result = successor_->id;
  peer_lock_.unlock();
  return true;
}

bool ChordIndex::handleGetPredecessor(PeerId* result) {
  if (!waitUntilInitialized()) {
    return false;
  }
  peer_lock_.readLock();
  *result = predecessor_->id;
  peer_lock_.unlock();
  return true;
}

bool ChordIndex::handleLock(const PeerId& requester) {
  std::lock_guard<std::mutex> lock(node_lock_);
  if (node_locked_) {
    return false;
  } else {
    node_locked_ = true;
    node_lock_holder_ = requester;
    VLOG(3) << hash(requester) << " locked " << own_key_;
    return true;
  }
}

bool ChordIndex::handleUnlock(const PeerId& requester) {
  std::lock_guard<std::mutex> lock(node_lock_);
  if (!node_locked_ || node_lock_holder_ != requester) {
    return false;
  } else {
    node_locked_ = false;
    VLOG(3) << hash(requester) << " unlocked " << own_key_;
    return true;
  }
}

bool ChordIndex::handleNotify(const PeerId& peer_id) {
  if (!waitUntilInitialized()) {
    return false;
  }
  if (FLAGS_join_mode == kCleanJoin) {
    return handleNotifyClean(peer_id);
  } else {
    CHECK_EQ(kStabilizeJoin, FLAGS_join_mode);
    return handleNotifyStabilize(peer_id);
  }
}

bool ChordIndex::handleAddData(
    const std::string& key, const std::string& value) {
  // TODO(tcies) try-again-later & integrate if not integrated
  return addDataLocally(key, value);
}

bool ChordIndex::handleRetrieveData(
    const std::string& key, std::string* value) {
  CHECK_NOTNULL(value);
  // TODO(tcies) try-again-later & integrate if not integrated
  if (!retrieveDataLocally(key, value)) {
    LOG(WARNING) << "Data " << key << " requested at " << PeerId::self() <<
        " is not available.";
    return false;
  }
  return true;
}

bool ChordIndex::handleFetchResponsibilities(
    const PeerId& requester, DataMap* responsibilities) {
  CHECK_NOTNULL(responsibilities);
  // TODO(tcies) try-again-later if not integrated
  data_lock_.readLock();
  for (const DataMap::value_type& item : data_) {
    if (!isIn(hash(item.first), hash(requester), own_key_)) {
      responsibilities->insert(item);
    }
  }
  data_lock_.unlock();
  return true;
}

bool ChordIndex::addData(const std::string& key, const std::string& value) {
  Key chord_key = hash(key);
  PeerId successor = findSuccessor(chord_key);
  if (successor == PeerId::self()) {
    return addDataLocally(key, value);
  } else {
    CHECK(addDataRpc(successor, key, value));
    return true;
  }
}

bool ChordIndex::retrieveData(const std::string& key, std::string* value) {
  CHECK_NOTNULL(value);
  Key chord_key = hash(key);
  PeerId successor = findSuccessor(chord_key);
  if (successor == PeerId::self()) {
    return retrieveDataLocally(key, value);
  } else {
    if (!retrieveDataRpc(successor, key, value)) {
      return false;
    }
    return true;
  }
}

PeerId ChordIndex::findSuccessor(const Key& key) {
  peer_lock_.readLock();
  PeerId result;
  if (isIn(key, own_key_, successor_->key)) {
    result = successor_->id;
    peer_lock_.unlock();
    return result;
  } else {
    peer_lock_.unlock();
    CHECK(getSuccessorRpc(findPredecessor(key), &result));
    return result;
  }
}

PeerId ChordIndex::findPredecessor(const Key& key) {
  peer_lock_.readLock();
  CHECK(!isIn(key, own_key_, successor_->key)) <<
      "FindPredecessor called while it's the calling peer";
  peer_lock_.unlock();
  PeerId result = closestPrecedingFinger(key), result_successor;
  CHECK(getSuccessorRpc(result, &result_successor));
  while (!isIn(key, hash(result), hash(result_successor))) {
    CHECK(getClosestPrecedingFingerRpc(result, key, &result));
    CHECK(getSuccessorRpc(result, &result_successor));
  }
  return result;
}

void ChordIndex::create() {
  init();
  peer_lock_.writeLock();
  for (size_t i = 0; i < M; ++i) {
    fingers_[i].peer = self_;
  }
  successor_ = self_;
  predecessor_ = self_;
  peer_lock_.unlock();
  std::lock_guard<std::mutex> integrate_lock(integrate_mutex_);
  integrated_ = true;

  std::unique_lock<std::mutex> lock(initialized_mutex_);
  initialized_ = true;
  lock.unlock();
  initialized_cv_.notify_all();
  LOG(INFO) << "Root has key " << own_key_;
}

void ChordIndex::join(const PeerId& other) {
  init();
  if (FLAGS_join_mode == kCleanJoin) {
    cleanJoin(other);
  } else {
    CHECK_EQ(kStabilizeJoin, FLAGS_join_mode);
    stabilizeJoin(other);
  }
  std::unique_lock<std::mutex> lock(initialized_mutex_);
  initialized_ = true;
  lock.unlock();
  initialized_cv_.notify_all();
  LOG(INFO) << "Peer has key " << own_key_;
}

void ChordIndex::cleanJoin(const PeerId& other) {
  // 1. lock proper predecessor and successor
  PeerId predecessor = other, successor;
  while (true) {

    CHECK(getClosestPrecedingFingerRpc(predecessor, own_key_, &predecessor));
    CHECK(getSuccessorRpc(predecessor, &successor));

    if (!isIn(own_key_, hash(predecessor), hash(successor))) {
      // case when predecessor.successor managed to join between the above
      // two RPC's
      continue;
    }

    // locking must occur in order, in order to avoid deadlock
    if (hash(predecessor) < hash(successor)) {
      CHECK(lock(predecessor));
      CHECK(lock(successor));
    } else if (hash(predecessor) > hash(successor)) {
      CHECK(lock(successor));
      CHECK(lock(predecessor));
    } else {
      CHECK_EQ(predecessor, successor) << "Same hash of two different peers";
      CHECK(lock(predecessor));
    }

    PeerId successor_predecessor, predecessor_successor;
    CHECK(getPredecessorRpc(successor, &successor_predecessor));
    CHECK(getSuccessorRpc(predecessor, &predecessor_successor));
    if (predecessor_successor == successor &&
        successor_predecessor == predecessor) {
      break;
    }

    unlock(predecessor);
    if (predecessor != successor) {
      unlock(successor);
    }
  }
  // 2. fetch data
  // TODO(tcies) atomicity: what if any request while locked?
  data_lock_.writeLock();
  CHECK(fetchResponsibilitiesRpc(successor, &data_));
  data_lock_.unlock();
  // 3. set pred/succ
  registerPeer(predecessor, &predecessor_);
  registerPeer(successor, &successor_);
  // 4. notify & unlock
  CHECK(notifyRpc(predecessor, PeerId::self()));
  if (predecessor != successor) {
    CHECK(notifyRpc(successor, PeerId::self()));
    unlock(successor);
  }
  unlock(predecessor);
}

void ChordIndex::stabilizeJoin(const PeerId& other) {
  registerPeer(other, &successor_);
  registerPeer(other, &predecessor_);
  //  LOG(INFO) << PeerId::self() << " stabilize-joined " << other;
}

bool ChordIndex::lock(const PeerId& subject) const {
  while (true) {
    if (!lockRpc(subject)) {
      usleep(1000);
    } else {
      break;
    }
  }
  return true;
}

void ChordIndex::unlock(const PeerId& subject) const {
  CHECK(unlockRpc(subject));
}

void ChordIndex::leave() {
  terminate_ = true;
  stabilizer_.join();
  // TODO(tcies) move data to successor
  usleep(5000); // TODO(tcies) unhack! "Ensures" that pending requests resolve
  initialized_ = false;
  initialized_cv_.notify_all();
  integrated_ = false;
}

PeerId ChordIndex::closestPrecedingFinger(
    const Key& key) {
  peer_lock_.readLock();
  PeerId result;
  if(isIn(key, own_key_, successor_->key)) {
    result = PeerId::self();
  } else {
    result = successor_->id;
  }
  peer_lock_.unlock();
  return result;
}

void ChordIndex::stabilizeThread(ChordIndex* self) {
  CHECK_NOTNULL(self);
  if (!self->waitUntilInitialized()) {
    return;
  }
  if (FLAGS_join_mode == kCleanJoin) {
    return;
  }
  while (!self->terminate_) {
    PeerId successor_predecessor;
    self->peer_lock_.readLock();
    if (self->successor_->id != PeerId::self()) {
      if (!self->getPredecessorRpc(self->successor_->id, &successor_predecessor)) {
        // Node leaves have not been accounted for yet. However, not crashing
        // the program is necessery for successful (simultaneous) shutdown of a
        // network.
        self->peer_lock_.unlock();
        continue;
      }
      if (successor_predecessor != PeerId::self() &&
          isIn(hash(successor_predecessor), self->own_key_,
               self->successor_->key)) {
        self->peer_lock_.unlock();
        self->registerPeer(successor_predecessor, &self->successor_);
        self->peer_lock_.readLock();
        VLOG(3) << self->own_key_ << " changed successor to " <<
            hash(successor_predecessor) << " through stabilization";
      }
      // because notifyRpc does peer_lock_.writeLock on the remote peer, we need
      // to release the read-lock to avoid potential deadlock.
      PeerId to_notify = self->successor_->id;
      self->peer_lock_.unlock();
      if (!self->notifyRpc(to_notify, PeerId::self())) {
        continue;
      }
      self->peer_lock_.readLock();
    }
    self->peer_lock_.unlock();
    usleep(FLAGS_stabilize_us);
    // TODO(tcies) finger fixing
  }
}

void ChordIndex::integrateThread(ChordIndex* self) {
  CHECK_NOTNULL(self);
  std::lock_guard<std::mutex> lock(self->integrate_mutex_);
  if (self->integrated_) {
    return;
  }
  // The assumption is made that successor is indeed the peer that contains the
  // required data. In general, this is valid because a peer gets notified
  // from its actual predecessor only once its true successor has registered it
  // as predecessor (for proof see report).
  // There is, however, a corner case in which this is not valid: Another
  // peer that is between this peer and its successor could join at the same
  // time as this peer sends this request. For now, this request should still
  // succeed as data is not being deleted once delegated. TODO(tcies) delete
  // data once delegated?
  self->data_lock_.writeLock();
  CHECK(self->fetchResponsibilitiesRpc(self->successor_->id, &self->data_));
  self->data_lock_.unlock();
  self->integrated_ = true;
}

void ChordIndex::init() {
  //  LOG(INFO) << "Initializing chord for " << PeerId::self();
  own_key_ = hash(PeerId::self());
  self_.reset(new ChordPeer(PeerId::self()));
  //  LOG(INFO) << "Self key is " << self_->key;
  for (size_t i = 0; i < M; ++i) {
    fingers_[i].base_key = own_key_ + (1 << i); // overflow intended
  }
  terminate_ = false;
  stabilizer_ = std::thread(stabilizeThread, this);
  data_lock_.writeLock();
  data_.clear();
  data_lock_.unlock();
  std::lock_guard<std::mutex> lock(node_lock_);
  node_locked_ = false;
}

void ChordIndex::registerPeer(
    const PeerId& peer, std::shared_ptr<ChordPeer>* target) {
  CHECK_NOTNULL(target);
  peer_lock_.writeLock();
  PeerMap::iterator found = peers_.find(peer);
  std::shared_ptr<ChordPeer> existing;
  if (found != peers_.end() && (existing = found->second.lock())){
    *target = existing;
  } else {
    target->reset(new ChordPeer(peer));
    peers_[peer] = std::weak_ptr<ChordPeer>(*target);
  }
  peer_lock_.unlock();
}

bool ChordIndex::isIn(
    const Key& key, const Key& from_inclusive, const Key& to_exclusive) {
  if (key == from_inclusive) {
    return true;
  }
  if (to_exclusive == from_inclusive) {
    return true;
  }
  if (from_inclusive <= to_exclusive) { // case doesn't pass 0
    return (from_inclusive < key && key < to_exclusive);
  } else { // case passes 0
    return (from_inclusive < key || key < to_exclusive);
  }
}

bool ChordIndex::waitUntilInitialized() {
  std::unique_lock<std::mutex> lock(initialized_mutex_);
  while (!initialized_) {
    if (terminate_) {
      return false;
    }
    initialized_cv_.wait(lock);
  }
  lock.unlock();
  initialized_cv_.notify_all();
  return true;
}

bool ChordIndex::addDataLocally(
    const std::string& key, const std::string& value) {
  data_lock_.readLock();
  if (data_.find(key) != data_.end()) {
    data_lock_.unlock();
    LOG(ERROR) << "Data with given key " << key << " already exists";
    return false;
  }
  data_lock_.unlock();
  data_lock_.writeLock();
  data_[key] = value;
  data_lock_.unlock();
  return true;
}

bool ChordIndex::retrieveDataLocally(
    const std::string& key, std::string* value) {
  CHECK_NOTNULL(value);
  data_lock_.readLock();
  DataMap::iterator found = data_.find(key);
  if (found == data_.end()) {
    data_lock_.unlock();
    LOG(ERROR) << "Data with given key " << key << " does not exist";
    return false;
  }
  *value = data_[key];
  data_lock_.unlock();
  return true;
}

bool ChordIndex::handleNotifyClean(const PeerId& peer_id) {
  CHECK(node_locked_);
  CHECK_EQ(peer_id, node_lock_holder_);
  peer_lock_.readLock();
  CHECK(peers_.find(peer_id) == peers_.end());
  std::shared_ptr<ChordPeer> peer(new ChordPeer(peer_id));
  handleNotifyCommon(peer);
  CHECK(peer.use_count() > 1);
  peer_lock_.unlock();
  peer_lock_.writeLock();
  peers_[peer_id] = std::weak_ptr<ChordPeer>(peer);
  peer_lock_.unlock();
  return true;
}

bool ChordIndex::handleNotifyStabilize(const PeerId& peer_id) {
  peer_lock_.readLock();
  // if notify came from predecessor, integrate!
  if (isIn(hash(peer_id), predecessor_->key, own_key_)) {
    // at this point we could start receiving data requests
    if (!integrated_) {
      // this has to be done in a separate thread in order to avoid deadlocks
      // of the kind of mutual waiting for response
      std::thread integrate_thread(integrateThread, this);
      integrate_thread.detach();
    }
  }
  if (peers_.find(peer_id) != peers_.end()) {
    // already aware of the node
    peer_lock_.unlock();
    return true;
  }
  std::shared_ptr<ChordPeer> peer(new ChordPeer(peer_id));
  handleNotifyCommon(peer);
  // save peer to peer map only if information has been useful anywhere
  if (peer.use_count() > 1) {
    peer_lock_.unlock();
    peer_lock_.writeLock();
    peers_[peer_id] = std::weak_ptr<ChordPeer>(peer);
    peer_lock_.unlock();
    peer_lock_.readLock();
    // TODO(tcies) how will it be removed?
  }
  peer_lock_.unlock();
  return true;
}

void ChordIndex::handleNotifyCommon(std::shared_ptr<ChordPeer> peer) {
  if (isIn(peer->key, own_key_, successor_->key)) {
    peer_lock_.unlock();
    peer_lock_.writeLock();
    successor_ = peer;
    peer_lock_.unlock();
    peer_lock_.readLock();
    VLOG(3) << own_key_ << " changed successor to " << peer->key <<
        " by notification";
  }
  // fix predecessor
  if (isIn(peer->key, predecessor_->key, own_key_)) {
    peer_lock_.unlock();
    peer_lock_.writeLock();
    predecessor_ = peer;
    peer_lock_.unlock();
    peer_lock_.readLock();
    VLOG(3) << own_key_ << " changed predecessor to " << peer->key <<
        " by notification";
  }
}

} /* namespace map_api */
