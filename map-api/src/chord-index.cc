#include <map-api/chord-index.h>
#include <type_traits>

#include <glog/logging.h>
#include <gflags/gflags.h>

const std::string kCleanJoin("clean");
const std::string kStabilizeJoin("stabilize");

DEFINE_string(join_mode, kCleanJoin,
              ("Can be " + kCleanJoin + " or " + kStabilizeJoin).c_str());
DEFINE_uint64(stabilize_us, 1000, "Interval of stabilization in microseconds");
DECLARE_int32(simulated_lag_ms);

namespace map_api {

constexpr bool enable_fingers = true;

ChordIndex::~ChordIndex() {}

bool ChordIndex::handleGetClosestPrecedingFinger(
    const Key& key, PeerId* result) {
  CHECK_NOTNULL(result);
  if (!waitUntilInitialized()) {
    // TODO(tcies) re-introduce request_status
    LOG(ERROR) << "Not active any more! Clean solution?";
  }
  *result = closestPrecedingFinger(key);
  return true;
}

bool ChordIndex::handleGetSuccessor(PeerId* result) {
  CHECK_NOTNULL(result);
  if (!waitUntilInitialized()) {
    // TODO(tcies) re-introduce request_status
    LOG(ERROR) << "Not active any more! Clean solution?";
  }
  peer_lock_.acquireReadLock();
  *result = successor_->id;
  peer_lock_.releaseReadLock();
  return true;
}

bool ChordIndex::handleGetPredecessor(PeerId* result) {
  if (!waitUntilInitialized()) {
    // TODO(tcies) re-introduce request_status
    LOG(ERROR) << "Not active any more! Clean solution?";
  }
  peer_lock_.acquireReadLock();
  *result = predecessor_->id;
  peer_lock_.releaseReadLock();
  return true;
}

bool ChordIndex::handleLock(const PeerId& requester) {
  if (!waitUntilInitialized()) {
    // TODO(tcies) re-introduce request_status
    LOG(ERROR) << "Not active any more! Clean solution?";
  }
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
  if (!waitUntilInitialized()) {
    // TODO(tcies) re-introduce request_status
    LOG(ERROR) << "Not active any more! Clean solution?";
  }
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
    // TODO(tcies) re-introduce request_status
    LOG(FATAL) << "Should NotifyRpc only locked peers, locked peers shouldn't "\
        "be able to leave!";
  }
  if (FLAGS_join_mode == kCleanJoin) {
    return handleNotifyClean(peer_id);
  } else {
    CHECK_EQ(kStabilizeJoin, FLAGS_join_mode);
    return handleNotifyStabilize(peer_id);
  }
}

bool ChordIndex::handleReplace(const PeerId& old_peer, const PeerId& new_peer) {
  if (!waitUntilInitialized()) {
    // TODO(tcies) re-introduce request_status
    LOG(FATAL) << "Should Repl.Rpc only locked peers, locked peers shouldn't "\
        "be able to leave!";
  }
  CHECK_EQ(kCleanJoin, FLAGS_join_mode) <<
      "Replace available only in clean join";
  peer_lock_.acquireReadLock();
  bool successor = old_peer == successor_->id;
  bool predecessor = old_peer == predecessor_->id;
  if (!successor && !predecessor) {  // could be both
    return false;
  }
  std::lock_guard<std::mutex> lock(node_lock_);
  if (successor) {
    if (!node_locked_ || node_lock_holder_ != old_peer) {
      peer_lock_.releaseReadLock();
      return false;
    }
  }
  if (predecessor) {
    if (!node_locked_ || node_lock_holder_ != old_peer) {
      peer_lock_.releaseReadLock();
      return false;
    }
  }
  peer_lock_.releaseReadLock();  // registerPeer does writeLock
  if (successor) {
    registerPeer(new_peer, &successor_);
  }
  if (predecessor) {
    registerPeer(new_peer, &predecessor_);
  }
  return true;
}

bool ChordIndex::handleAddData(
    const std::string& key, const std::string& value) {
  if (!waitUntilInitialized()) {
    // TODO(tcies) re-introduce request_status
    LOG(FATAL) << "Need to implement request status for departed peers.";
  }
  // TODO(tcies) try-again-later & integrate if not integrated
  return addDataLocally(key, value);
}

bool ChordIndex::handleRetrieveData(
    const std::string& key, std::string* value) {
  CHECK_NOTNULL(value);
  if (!waitUntilInitialized()) {
    // TODO(tcies) re-introduce request_status
    LOG(FATAL) << "Need to implement request status for departed peers.";
  }
  // TODO(tcies) try-again-later & integrate if not integrated
  if (!retrieveDataLocally(key, value)) {
    VLOG(3) << "Data " << key << " requested at " << PeerId::self()
            << " is not available, has hash " << hash(key);
    return false;
  }
  return true;
}

bool ChordIndex::handleFetchResponsibilities(
    const PeerId& requester, DataMap* responsibilities) {
  CHECK_NOTNULL(responsibilities);
  if (!waitUntilInitialized()) {
    // TODO(tcies) re-introduce request_status
    LOG(FATAL) << "Should FRRpc only locked peers, locked peers shouldn't "\
        "be able to leave!";
  }
  // TODO(tcies) try-again-later if not integrated
  data_lock_.acquireReadLock();
  for (const DataMap::value_type& item : data_) {
    if (!isIn(hash(item.first), hash(requester), own_key_)) {
      responsibilities->insert(item);
    }
  }
  data_lock_.releaseReadLock();
  return true;
}

bool ChordIndex::handlePushResponsibilities(const DataMap& responsibilities) {
  if (!waitUntilInitialized()) {
    // TODO(tcies) re-introduce request_status
    LOG(FATAL) << "Should PRRpc only locked peers, locked peers shouldn't "\
        "be able to leave!";
  }
  data_lock_.acquireWriteLock();
  for (const DataMap::value_type& item : responsibilities) {
    data_[item.first] = item.second;  // overwrite intended
  }
  data_lock_.releaseWriteLock();
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
  peer_lock_.acquireReadLock();
  PeerId result;
  if (isIn(key, own_key_, successor_->key)) {
    result = successor_->id;
    peer_lock_.releaseReadLock();
    return result;
  } else {
    peer_lock_.releaseReadLock();
    CHECK(getSuccessorRpc(findPredecessor(key), &result));
    return result;
  }
}

PeerId ChordIndex::findPredecessor(const Key& key) {
  peer_lock_.acquireReadLock();
  CHECK(!isIn(key, own_key_, successor_->key)) <<
      "FindPredecessor called while it's the calling peer";
  peer_lock_.releaseReadLock();
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
  peer_lock_.acquireWriteLock();
  if (!enable_fingers) {
    for (size_t i = 0; i < kNumFingers; ++i) {
      fingers_[i].peer = self_;
    }
  }
  successor_ = self_;
  predecessor_ = self_;
  peer_lock_.releaseWriteLock();
  std::lock_guard<std::mutex> integrate_lock(integrate_mutex_);
  integrated_ = true;

  std::unique_lock<std::mutex> lock(initialized_mutex_);
  initialized_ = true;
  lock.unlock();
  initialized_cv_.notify_all();
  VLOG(3) << "Root(" << PeerId::self() << ") has key " << own_key_;
}

bool ChordIndex::join(const PeerId& other) {
  init();
  if (FLAGS_join_mode == kCleanJoin) {
    if (!cleanJoin(other)) {
      return false;
    }
  } else {
    CHECK_EQ(kStabilizeJoin, FLAGS_join_mode);
    stabilizeJoin(other);
  }
  std::unique_lock<std::mutex> lock(initialized_mutex_);
  initialized_ = true;
  lock.unlock();
  initialized_cv_.notify_all();
  VLOG(3) << "Peer(" << PeerId::self() << ") has key " << own_key_;
  return true;
}

bool ChordIndex::cleanJoin(const PeerId& other) {
  // 1. lock proper predecessor and successor
  PeerId predecessor = other, successor;
  while (true) {
    if (!getClosestPrecedingFingerRpc(predecessor, own_key_, &predecessor)) {
      return false;
    }
    if (!getSuccessorRpc(predecessor, &successor)) {
      return false;
    }
    if (!isIn(own_key_, hash(predecessor), hash(successor))) {
      // case when predecessor.successor managed to join between the above
      // two RPC's
      continue;
    }

    if (!lockPeersInOrder(predecessor, successor)) {
      return false;
    }
    PeerId successor_predecessor, predecessor_successor;
    if (!(getPredecessorRpc(successor, &successor_predecessor) &&
          getSuccessorRpc(predecessor, &predecessor_successor))) {
      unlockPeers(predecessor, successor);
      return false;
    }
    if (predecessor_successor == successor &&
        successor_predecessor == predecessor) {
      break;
    }
    unlockPeers(predecessor, successor);
  }

  joinBetweenLockedPeers(predecessor, successor);
  return true;
}

void ChordIndex::stabilizeJoin(const PeerId& other) {
  registerPeer(other, &successor_);
  registerPeer(other, &predecessor_);
  //  LOG(INFO) << PeerId::self() << " stabilize-joined " << other;
}

bool ChordIndex::lock() {
  while (true) {
    node_lock_.lock();
    if (!node_locked_) {
      node_locked_ = true;
      node_lock_holder_ = PeerId::self();
      VLOG(3) << own_key_ << " locked to self";
      node_lock_.unlock();
      break;
    } else {
      node_lock_.unlock();
      usleep(1000);
    }
  }
  return true;
}

bool ChordIndex::lock(const PeerId& subject) {
  while (true) {
    if (lockRpc(subject) != RpcStatus::SUCCESS) {
      usleep(1000);
    } else {
      break;
    }
  }
  return true;
}

void ChordIndex::unlock() {
  std::lock_guard<std::mutex> lock(node_lock_);
  CHECK(node_locked_);
  CHECK(node_lock_holder_ == PeerId::self());
  node_locked_ = false;
  VLOG(3) << own_key_ << " unlocked self";
}

bool ChordIndex::unlock(const PeerId& subject) {
  return unlockRpc(subject) == RpcStatus::SUCCESS;
}

bool ChordIndex::lockPeersInOrder(const PeerId& subject_1,
                                  const PeerId& subject_2) {
  // Locking must occur in order in order to avoid deadlocks.
  if (hash(subject_1) < hash(subject_2)) {
    if (!lock(subject_1)) {
      return false;
    }
    if (!lock(subject_2)) {
      unlock(subject_1);
      return false;
    }
    return true;
  } else if (hash(subject_1) > hash(subject_2)) {
    if (!lock(subject_2)) {
      return false;
    }
    if (!lock(subject_1)) {
      unlock(subject_2);
      return false;
    }
    return true;
  } else {
    CHECK_EQ(subject_1, subject_2) << "Same hash of two different peers";
    return lock(subject_1);
  }
  return false;
}

bool ChordIndex::unlockPeers(const PeerId& subject_1, const PeerId& subject_2) {
  bool unlock_result = unlock(subject_1);
  if (subject_1 != subject_2) {
    unlock_result = (unlock(subject_2) && unlock_result);
  }
  return unlock_result;
}

void ChordIndex::leave() {
  terminate_ = true;
  stabilizer_.join();
  // TODO(aqurai): Check if this has to be uncommented.
  // CHECK_EQ(kCleanJoin, FLAGS_join_mode) << "Stabilize leave deprecated";
  leaveClean();
  // TODO(tcies) unhack! "Ensures" that pending requests resolve
  usleep(FLAGS_simulated_lag_ms * 100000 + 100000);
  initialized_ = false;
  initialized_cv_.notify_all();
  integrated_ = false;
}

void ChordIndex::leaveClean() {
  PeerId predecessor, successor;
  // 1. acquire locks
  while (true) {
    peer_lock_.acquireReadLock();
    predecessor = predecessor_->id;
    successor = successor_->id;
    peer_lock_.releaseReadLock();
    // in-order locking
    if (hash(successor) <= hash(predecessor)) {
      if (hash(successor) < hash(predecessor)) {
        CHECK_NE(PeerId::self(), successor);
        CHECK_NE(PeerId::self(), predecessor);
        if (own_key_ > hash(successor)) {  // su ... pr, self
          CHECK(lock(successor));
          CHECK(lock(predecessor));
          CHECK(lock());
        } else {  // self, su ... pr
          CHECK(lock());
          CHECK(lock(successor));
          CHECK(lock(predecessor));
        }
      } else {
        CHECK_EQ(successor, predecessor);
        if (own_key_ < hash(successor)) {  // self, su = pr
          CHECK(lock());
          CHECK(lock(successor));
        } else if (hash(successor) < own_key_) {  // su = pr, self
          CHECK(lock(successor));
          CHECK(lock());
        } else {  // su = pr = self
          CHECK_EQ(PeerId::self(), predecessor);
          CHECK(lock());
        }
      }
    } else {  // general case: ... pr, self, su ...
      CHECK_NE(PeerId::self(), successor);
      CHECK_NE(PeerId::self(), predecessor);
      CHECK(lock(predecessor));
      CHECK(lock());
      CHECK(lock(successor));
    }
    // verify validity
    if (predecessor == PeerId::self()) {
      if (successor_->id == PeerId::self() &&
          predecessor_->id == PeerId::self()) {
        break;
      }
    } else {
      bool predecessor_consistent, self_consistent, successor_consistent;
      PeerId predecessor_successor, successor_predecessor;
      CHECK(getSuccessorRpc(predecessor, &predecessor_successor));
      predecessor_consistent = predecessor_successor == PeerId::self();
      peer_lock_.acquireReadLock();
      self_consistent = predecessor_->id == predecessor &&
          successor_->id == successor;
      peer_lock_.releaseReadLock();
      CHECK(getPredecessorRpc(successor, &successor_predecessor));
      successor_consistent = successor_predecessor == PeerId::self();
      if (predecessor_consistent && self_consistent && successor_consistent) {
        break;
      }
    }
    // unlock, repeat
    unlock(predecessor);
    unlock();
    if (successor != predecessor) {
      unlock(successor);
    }
  }
  if (successor != PeerId::self()) {
    // 2. push data
    data_lock_.acquireReadLock();
    pushResponsibilitiesRpc(successor, data_);
    data_lock_.releaseReadLock();
    // 3. reroute & unlock
    CHECK(replaceRpc(successor, PeerId::self(), predecessor));
    if (successor != predecessor) {
      CHECK(replaceRpc(predecessor, PeerId::self(), successor));
      unlock(predecessor);
    }
    unlock(successor);
    VLOG(3) << own_key_ << " left ring topo";
  } else {
    VLOG(3) << "Last peer left chord index";
  }
  unlock();
}

void ChordIndex::localUpdateCallback(const std::string& /*key*/,
                                     const std::string& /*old_value*/,
                                     const std::string& /*new_value*/) {}

PeerId ChordIndex::closestPrecedingFinger(const Key& key) {
  peer_lock_.acquireReadLock();
  PeerId result;
  if (isIn(key, own_key_, successor_->key)) {
    result = PeerId::self();
  } else {
    result = successor_->id;
    if (enable_fingers) {
      for (size_t i = 1; i < kNumFingers; ++i) {
        if (!fingers_[i].peer->isValid()) {
          break;
        }
        if (!isIn(key, fingers_[i - 1].peer->key, fingers_[i].peer->key)) {
          result = fingers_[i].peer->id;
        } else {
          break;
        }
      }
    }
  }
  peer_lock_.releaseReadLock();
  return result;
}

void ChordIndex::stabilizeThread(ChordIndex* self) {
  CHECK_NOTNULL(self);
  if (!self->waitUntilInitialized()) {
    return;
  }
  usleep(5000);

  if (enable_fingers) {
    for (size_t i = 0; i < kNumFingers; ++i) {
      self->fixFinger(i);
      // Give time for other RPCs to proceed.
      usleep(1000);
    }
  }

  int fix_finger_index = 0;
  while (!self->terminate_) {
    // Avoid holding lock during RPC.
    self->peer_lock_.acquireReadLock();
    const PeerId successor_id = self->successor_->id;
    const Key own_key = self->own_key_;
    const Key successor_key = self->successor_->key;
    self->peer_lock_.releaseReadLock();

    PeerId successor_predecessor;
    if (successor_id != PeerId::self()) {
      bool successor_responding =
          self->getPredecessorRpc(successor_id, &successor_predecessor);

      if (!successor_responding) {
        self->replaceDisconnectedSuccessor();
      } else {
        self->lock();
        if (successor_predecessor != PeerId::self() &&
            isIn(hash(successor_predecessor), own_key, successor_key)) {
          self->unlock();
          // Someone joined in between.
          // TODO(aqurai): Check if this case ever happens after joinBetween
          // impl.
          LOG(WARNING) << "Someone joined in between me and my successor.";
          self->lock(successor_predecessor);
          self->registerPeer(successor_predecessor, &self->successor_);
          self->data_lock_.acquireWriteLock();
          CHECK(self->fetchResponsibilitiesRpc(successor_predecessor,
                                               &self->data_));
          self->data_lock_.releaseWriteLock();
          bool result = self->notifyRpc(successor_predecessor, PeerId::self());
          self->unlock(successor_predecessor);
          if (!result) {
            continue;
          }
        } else if (successor_predecessor != PeerId::self() &&
                   isIn(own_key, hash(successor_predecessor), successor_key)) {
          self->unlock();
          // Chord ring bypasses this peer. Can happen when this peer gets
          // disconnected for a while.
          // TODO(aqurai): Verify and test this.
          LOG(FATAL) << "No tests written for producing this, hence this "
                        "shouldnt happen.";
          self->peer_lock_.releaseReadLock();
          self->lockPeersInOrder(successor_predecessor, successor_id);
          self->joinBetweenLockedPeers(successor_predecessor, successor_id);
          self->peer_lock_.acquireReadLock();
        } else {
          self->unlock();
        }
      }
    }

    // TODO(aqurai): Debugging purpose. Remove later.
    if (VLOG_IS_ON(2)) {
      bool print = false;
      std::stringstream finger_list;
      for (size_t i = 0; i < kNumFingers; ++i) {
        common::ScopedReadLock(&self->peer_lock_);
        if (!self->fingers_[i].peer->isValid()) {
          if (i < 2) {
            print = true;
          }
          finger_list << " " << i;
        }
      }
      // Don't print if only higher fingers are invalid.
      if (print) {
        LOG(WARNING) << " Fingers " << finger_list.str()
                     << " not initialized on " << PeerId::self();
      }
    }

    if (enable_fingers) {
      self->fixFinger(fix_finger_index);
      ++fix_finger_index;
      if (fix_finger_index == kNumFingers) {
        fix_finger_index = 0;
      }
    }
    usleep(FLAGS_stabilize_us);
  }
}

bool ChordIndex::joinBetweenLockedPeers(const PeerId& predecessor,
                                        const PeerId& successor) {
  // Fetch data.
  data_lock_.acquireWriteLock();
  if (!fetchResponsibilitiesRpc(successor, &data_)) {
    unlockPeers(predecessor, successor);
    return false;
  }
  data_lock_.releaseWriteLock();
  // TODO(aqurai): Mark: Fetch replication data here.

  // Set pred/succ.
  registerPeer(predecessor, &predecessor_);
  registerPeer(successor, &successor_);

  // Notify & unlock.
  bool result = true;
  // TODO(aqurai) Test by only sending notification to successor.
  result = notifyRpc(predecessor, PeerId::self());
  if (predecessor != successor) {
    result = notifyRpc(successor, PeerId::self()) && result;
    result = unlock(successor) && result;
  }
  result = unlock(predecessor) && result;
  return result;
}

bool ChordIndex::replaceDisconnectedSuccessor() {
  LOG(WARNING) << "replaceDisconnectedSuccessor on " << PeerId::self();
  PeerId candidate;
  PeerId candidate_predecessor;

  peer_lock_.acquireReadLock();
  const PeerId self_successor = successor_->id;
  size_t finger_index = 0;

  // Get the peer corresponding to the lowest finger that is alive.
  for (size_t i = 0; i < kNumFingers; ++i) {
    if (!fingers_[i].peer->isValid()) {
      peer_lock_.releaseReadLock();
      return false;
    }
    const PeerId to = fingers_[i].peer->id;
    peer_lock_.releaseReadLock();
    bool response = getPredecessorRpc(to, &candidate_predecessor);
    peer_lock_.acquireReadLock();
    if (response) {
      finger_index = i;
      candidate = fingers_[i].peer->id;
      break;
    }
  }
  peer_lock_.releaseReadLock();

  if (!candidate_predecessor.isValid()) {
    // TODO(aqurai): Handle this. This means none of the getPred Rpcs succeeded.
    LOG(FATAL) << "Case of self disconnect from network not handled.";
    return false;
  }
  if (finger_index == 0) {
    return true;
  }
  if (!lock(candidate)) {
    return false;
  }

  // TODO(aqurai): Can this lead to infinite loop?
  size_t i = 0u;
  peer_lock_.acquireReadLock();

  // From the finger-peer, traverse back each predecessor towards self in the
  // circle. The first responding predecessor-peer becomes the new successor.
  // Locks help avoid simultaneous peer join at this point.
  PeerId candidate_predecessor_predecessor;
  while (candidate_predecessor != successor_->id) {
    ++i;
    if (i > 20) {
      LOG(WARNING) << "apparently inf loop in replaceDisconnectedSuccessor on "
                   << PeerId::self();
    }
    peer_lock_.releaseReadLock();
    bool response = getPredecessorRpc(candidate_predecessor,
                                      &candidate_predecessor_predecessor);
    if (!response) {
      break;
    } else {
      unlock(candidate);
      candidate = candidate_predecessor;
      candidate_predecessor = candidate_predecessor_predecessor;
      if (!lock(candidate)) {
        return false;
      }
    }
    peer_lock_.acquireReadLock();
  }

  // Set the successor and notify.
  if (candidate != successor_->id) {
    peer_lock_.releaseReadLock();
    registerPeer(candidate, &successor_);
    data_lock_.acquireWriteLock();
    CHECK(fetchResponsibilitiesRpc(candidate, &data_));
    data_lock_.releaseWriteLock();
    bool result = notifyRpc(candidate, PeerId::self());
    unlock(candidate);
    return result;
  }
  peer_lock_.releaseReadLock();
  return false;
}

void ChordIndex::fixFinger(size_t i) {
  if (i == 0) {
    common::ScopedWriteLock peer_lock(&peer_lock_);
    fingers_[0].peer.reset(new ChordPeer(successor_->id));
    return;
  }
  peer_lock_.acquireReadLock();
  const Key finger_key = fingers_[i].base_key;
  peer_lock_.releaseReadLock();

  const PeerId finger_peer = findSuccessor(finger_key);
  if (finger_peer != PeerId::self()) {
    setFingerPeer(finger_peer, &fingers_[i].peer);
  } else {
    peer_lock_.acquireWriteLock();
    fingers_[i].peer->invalidate();
    peer_lock_.releaseWriteLock();
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
  self->data_lock_.acquireWriteLock();
  CHECK(self->fetchResponsibilitiesRpc(self->successor_->id, &self->data_));
  self->data_lock_.releaseWriteLock();
  self->integrated_ = true;
}

void ChordIndex::init() {
  //  LOG(INFO) << "Initializing chord for " << PeerId::self();
  own_key_ = hash(PeerId::self());
  self_.reset(new ChordPeer(PeerId::self()));
  //  LOG(INFO) << "Self key is " << self_->key;
  for (size_t i = 0; i < kNumFingers; ++i) {
    fingers_[i].base_key = own_key_ + (1 << i);  // overflow intended
    fingers_[i].peer.reset(new ChordPeer());
  }
  terminate_ = false;
  stabilizer_ = std::thread(stabilizeThread, this);
  data_lock_.acquireWriteLock();
  data_.clear();
  data_lock_.releaseWriteLock();
  std::lock_guard<std::mutex> lock(node_lock_);
  node_locked_ = false;
}

void ChordIndex::registerPeer(
    const PeerId& peer, std::shared_ptr<ChordPeer>* target) {
  CHECK_NOTNULL(target);
  peer_lock_.acquireWriteLock();
  PeerMap::iterator found = peers_.find(peer);
  std::shared_ptr<ChordPeer> existing;
  if (found != peers_.end() && (existing = found->second.lock())) {
    *target = existing;
  } else {
    target->reset(new ChordPeer(peer));
    peers_[peer] = std::weak_ptr<ChordPeer>(*target);
  }
  peer_lock_.releaseWriteLock();
}

void ChordIndex::setFingerPeer(const PeerId& peer,
                               std::shared_ptr<ChordPeer>* target) {
  CHECK_NOTNULL(target);
  common::ScopedWriteLock peer_lock(&peer_lock_);
  target->reset(new ChordPeer(peer));
}

bool ChordIndex::isIn(
    const Key& key, const Key& from_inclusive, const Key& to_exclusive) {
  if (key == from_inclusive) {
    return true;
  }
  if (to_exclusive == from_inclusive) {
    return true;
  }
  if (from_inclusive <= to_exclusive) {  // case doesn't pass 0
    return (from_inclusive < key && key < to_exclusive);
  } else {  // case passes 0
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

bool ChordIndex::areFingersReady() {
  for (size_t i = 0; i < kNumFingers; ++i) {
    common::ScopedReadLock peer_lock(&peer_lock_);
    if (!fingers_[i].peer->isValid()) {
      return false;
    }
  }
  return true;
}

bool ChordIndex::addDataLocally(
    const std::string& key, const std::string& value) {
  data_lock_.acquireWriteLock();
  const std::string old_value = data_[key];
  data_[key] = value;
  // Releasing lock here so the update callback can do whatever it wants to.
  data_lock_.releaseWriteLock();
  localUpdateCallback(key, old_value, value);
  return true;
}

bool ChordIndex::retrieveDataLocally(
    const std::string& key, std::string* value) {
  CHECK_NOTNULL(value);
  data_lock_.acquireReadLock();
  DataMap::iterator found = data_.find(key);
  if (found == data_.end()) {
    data_lock_.releaseReadLock();
    return false;
  }
  *value = found->second;
  data_lock_.releaseReadLock();
  return true;
}

bool ChordIndex::handleNotifyClean(const PeerId& peer_id) {
  CHECK(node_locked_);
  CHECK_EQ(peer_id, node_lock_holder_);
  peer_lock_.acquireReadLock();
  CHECK(peers_.find(peer_id) == peers_.end());
  std::shared_ptr<ChordPeer> peer(new ChordPeer(peer_id));
  handleNotifyCommon(peer);
  CHECK_GT(peer.use_count(), 1);
  peer_lock_.releaseReadLock();
  peer_lock_.acquireWriteLock();
  peers_[peer_id] = std::weak_ptr<ChordPeer>(peer);
  peer_lock_.releaseWriteLock();
  return true;
}

bool ChordIndex::handleNotifyStabilize(const PeerId& peer_id) {
  peer_lock_.acquireReadLock();
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
    peer_lock_.releaseReadLock();
    return true;
  }
  std::shared_ptr<ChordPeer> peer(new ChordPeer(peer_id));
  handleNotifyCommon(peer);
  // save peer to peer map only if information has been useful anywhere
  if (peer.use_count() > 1) {
    peer_lock_.releaseReadLock();
    peer_lock_.acquireWriteLock();
    peers_[peer_id] = std::weak_ptr<ChordPeer>(peer);
    peer_lock_.releaseWriteLock();
    // TODO(tcies) how will it be removed?
  } else {
    peer_lock_.releaseReadLock();
  }
  return true;
}

void ChordIndex::handleNotifyCommon(std::shared_ptr<ChordPeer> peer) {
  if (isIn(peer->key, own_key_, successor_->key)) {
    peer_lock_.releaseReadLock();
    peer_lock_.acquireWriteLock();
    successor_ = peer;
    peer_lock_.releaseWriteLock();
    peer_lock_.acquireReadLock();
    VLOG(3) << own_key_ << " changed successor to " << peer->key <<
        " by notification";
  }
  // fix predecessor
  if (isIn(peer->key, predecessor_->key, own_key_)) {
    peer_lock_.releaseReadLock();
    peer_lock_.acquireWriteLock();
    predecessor_ = peer;
    peer_lock_.releaseWriteLock();
    peer_lock_.acquireReadLock();
    VLOG(3) << own_key_ << " changed predecessor to " << peer->key <<
        " by notification";
  }
}

} /* namespace map_api */
