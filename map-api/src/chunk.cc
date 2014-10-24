#include <map-api/chunk.h>
#include <fstream>  // NOLINT
#include <unordered_set>

#include <multiagent-mapping-common/conversions.h>
#include <timing/timer.h>

#include "./core.pb.h"
#include "./chunk.pb.h"
#include <map-api/cru-table.h>
#include <map-api/hub.h>
#include <map-api/net-table-manager.h>

enum UnlockStrategy {
  REVERSE,
  FORWARD,
  RANDOM
};
DEFINE_uint64(unlock_strategy, 2,
              "0: reverse of lock ordering, 1: same as"
              "lock ordering, 2: randomized");
DEFINE_bool(writelock_persist, true,
            "Enables more persisting write lock strategy");

namespace map_api {

const char Chunk::kConnectRequest[] = "map_api_chunk_connect";
const char Chunk::kInitRequest[] = "map_api_chunk_init_request";
const char Chunk::kInsertRequest[] = "map_api_chunk_insert";
const char Chunk::kLeaveRequest[] = "map_api_chunk_leave_request";
const char Chunk::kLockRequest[] = "map_api_chunk_lock_request";
const char Chunk::kNewPeerRequest[] = "map_api_chunk_new_peer_request";
const char Chunk::kUnlockRequest[] = "map_api_chunk_unlock_request";
const char Chunk::kUpdateRequest[] = "map_api_chunk_update_request";

MAP_API_PROTO_MESSAGE(Chunk::kConnectRequest, proto::ChunkRequestMetadata);
MAP_API_PROTO_MESSAGE(Chunk::kInitRequest, proto::InitRequest);
MAP_API_PROTO_MESSAGE(Chunk::kInsertRequest, proto::PatchRequest);
MAP_API_PROTO_MESSAGE(Chunk::kLeaveRequest, proto::ChunkRequestMetadata);
MAP_API_PROTO_MESSAGE(Chunk::kLockRequest, proto::ChunkRequestMetadata);
MAP_API_PROTO_MESSAGE(Chunk::kNewPeerRequest, proto::NewPeerRequest);
MAP_API_PROTO_MESSAGE(Chunk::kUnlockRequest, proto::ChunkRequestMetadata);
MAP_API_PROTO_MESSAGE(Chunk::kUpdateRequest, proto::PatchRequest);

const char Chunk::kLockSequenceFile[] = "meas_lock_sequence.txt";

template<>
void Chunk::fillMetadata<proto::ChunkRequestMetadata>(
    proto::ChunkRequestMetadata* destination) {
  CHECK_NOTNULL(destination);
  destination->set_table(underlying_table_->name());
  destination->set_chunk_id(id().hexString());
}

bool Chunk::init(const Id& id, CRTable* underlying_table, bool initialize) {
  CHECK_NOTNULL(underlying_table);
  id_ = id;
  underlying_table_ = underlying_table;
  initialized_ = initialize;
  return true;
}

bool Chunk::init(
    const Id& id, const proto::InitRequest& init_request, const PeerId& sender,
    CRTable* underlying_table) {
  CHECK(init(id, underlying_table, false));
  CHECK_GT(init_request.peer_address_size(), 0);
  for (int i = 0; i < init_request.peer_address_size(); ++i) {
    peers_.add(PeerId(init_request.peer_address(i)));
  }
  // feed data from connect_response into underlying table TODO(tcies) piecewise
  for (int i = 0; i < init_request.serialized_items_size(); ++i) {
    if (underlying_table->type() == CRTable::Type::CR) {
      std::shared_ptr<proto::Revision> raw_revision(new proto::Revision);
      CHECK(raw_revision->ParseFromString(init_request.serialized_items(i)));
      std::shared_ptr<Revision> data = std::make_shared<Revision>(raw_revision);
      CHECK(underlying_table->patch(data));
      syncLatestCommitTime(*data);
    } else {
      CHECK(underlying_table->type() == CRTable::Type::CRU);
      proto::History history_proto;
      CHECK(history_proto.ParseFromString(init_request.serialized_items(i)));
      CHECK_GT(history_proto.revisions_size(), 0);
      while (history_proto.revisions_size() > 0) {
        // using ReleaseLast allows zero-copy ownership transfer to the revision
        // object.
        std::shared_ptr<Revision> data = std::make_shared<Revision>(
            std::shared_ptr<proto::Revision>(
            history_proto.mutable_revisions()->ReleaseLast()));
        CHECK(underlying_table->patch(data));
        // TODO(tcies) guarantee order, then only sync latest time
        syncLatestCommitTime(*data);
      }
    }
  }
  std::lock_guard<std::mutex> metalock(lock_.mutex);
  lock_.preempted_state = DistributedRWLock::State::UNLOCKED;
  lock_.state = DistributedRWLock::State::WRITE_LOCKED;
  lock_.holder = sender;
  initialized_ = true;
  // Because it would be wasteful to iterate over all entries to find the
  // actual latest time:
  return true;
}

void Chunk::dumpItems(const LogicalTime& time, CRTable::RevisionMap* items) {
  CHECK_NOTNULL(items);
  distributedReadLock();
  underlying_table_->dumpChunk(id(), time, items);
  distributedUnlock();
}

size_t Chunk::numItems(const LogicalTime& time) {
  distributedReadLock();
  size_t result = underlying_table_->countByChunk(id(), time);
  distributedUnlock();
  return result;
}

size_t Chunk::itemsSizeBytes(const LogicalTime& time) {
  CRTable::RevisionMap items;
  distributedReadLock();
  underlying_table_->dumpChunk(id(), time, &items);
  distributedUnlock();
  size_t num_bytes = 0;
  for (const std::pair<Id, std::shared_ptr<const Revision> >& item : items) {
    CHECK(item.second != nullptr);
    const Revision& revision = *item.second;
    num_bytes += revision.byteSize();
  }
  return num_bytes;
}

// TODO(tcies) cache? : Store commit times with chunks as commits occur,
// share this info consistently.
void Chunk::getCommitTimes(const LogicalTime& sample_time,
                           std::set<LogicalTime>* commit_times) {
  CHECK_NOTNULL(commit_times);
  //  Using a temporary unordered map because it should have a faster insertion
  //  time. The expected amount of commit times << the expected amount of items,
  //  so this should be worth it.
  std::unordered_set<LogicalTime> unordered_commit_times;
  CRTable::RevisionMap items;
  CRUTable::HistoryMap histories;
  distributedReadLock();
  if (underlying_table_->type() == CRTable::Type::CR) {
    underlying_table_->dumpChunk(id(), sample_time, &items);
  } else {
    CHECK(underlying_table_->type() == CRTable::Type::CRU);
    CRUTable* table = static_cast<CRUTable*>(underlying_table_);
    table->chunkHistory(id(), sample_time, &histories);
  }
  distributedUnlock();
  if (underlying_table_->type() == CRTable::Type::CR) {
    for (const CRTable::RevisionMap::value_type& item : items) {
      unordered_commit_times.insert(item.second->getInsertTime());
    }
  } else {
    CHECK(underlying_table_->type() == CRTable::Type::CRU);
    for (const CRUTable::HistoryMap::value_type& history : histories) {
      for (const std::shared_ptr<const Revision>& revision : history.second) {
        unordered_commit_times.insert(revision->getUpdateTime());
      }
    }
  }
  commit_times->insert(unordered_commit_times.begin(),
                       unordered_commit_times.end());
}

bool Chunk::insert(const LogicalTime& time,
                   const std::shared_ptr<Revision>& item) {
  CHECK(item != nullptr);
  item->setChunkId(id());
  proto::PatchRequest insert_request;
  fillMetadata(&insert_request);
  Message request;
  distributedReadLock();  // avoid adding of new peers while inserting
  underlying_table_->insert(time, item);
  // at this point, insert() has modified the revision such that all default
  // fields are also set, which allows remote peers to just patch the revision
  // into their table.
  insert_request.set_serialized_revision(item->serializeUnderlying());
  request.impose<kInsertRequest>(insert_request);
  CHECK(peers_.undisputableBroadcast(&request));
  syncLatestCommitTime(*item);
  distributedUnlock();
  return true;
}

int Chunk::peerSize() const {
  return peers_.size();
}

void Chunk::enableLockLogging() {
  log_locking_ = true;
  self_rank_ = PeerId::selfRank();
  std::ofstream file(kLockSequenceFile, std::ios::out | std::ios::trunc);
  global_start_ = std::chrono::system_clock::now();
  current_state_ = UNLOCKED;
  main_thread_id_ = std::this_thread::get_id();
}

void Chunk::leave() {
  Message request;
  proto::ChunkRequestMetadata metadata;
  fillMetadata(&metadata);
  request.impose<kLeaveRequest>(metadata);
  distributedWriteLock();
  // leaving must be atomic wrt request handlers to prevent conflicts
  // this must happen after acquring the write lock to avoid deadlocks, should
  // two peers try to leave at the same time.
  leave_lock_.writeLock();
  CHECK(peers_.undisputableBroadcast(&request));
  relinquished_ = true;
  leave_lock_.unlock();
  distributedUnlock();  // i.e. must be able to handle unlocks from outside
  // the swarm. Should this pose problems in the future, we could tie unlocking
  // to leaving.
}

void Chunk::writeLock() { distributedWriteLock(); }

void Chunk::readLock() { distributedReadLock(); }

bool Chunk::isLocked() {
  std::lock_guard<std::mutex> metalock(lock_.mutex);
  return isWriter(PeerId::self()) && lock_.thread == std::this_thread::get_id();
}

void Chunk::unlock() { distributedUnlock(); }

// not expressing in terms of the peer-specifying overload in order to avoid
// unnecessary distributed lock and unlocks
int Chunk::requestParticipation() {
  distributedWriteLock();
  size_t new_participant_count = addAllPeers();
  distributedUnlock();
  return new_participant_count;
}

int Chunk::requestParticipation(const PeerId& peer) {
  CHECK(Hub::instance().hasPeer(peer));
  int new_participant_count = 0;
  distributedWriteLock();
  std::set<PeerId> hub_peers;
  Hub::instance().getPeers(&hub_peers);
  if (peers_.peers().find(peer) == peers_.peers().end()) {
    if (addPeer(peer)) {
      ++new_participant_count;
    }
  }
  distributedUnlock();
  return new_participant_count;
}

void Chunk::update(const std::shared_ptr<Revision>& item) {
  CHECK(item != nullptr);
  CHECK(underlying_table_->type() == CRTable::Type::CRU);
  CRUTable* table = static_cast<CRUTable*>(underlying_table_);
  CHECK_EQ(id(), item->getChunkId());
  proto::PatchRequest update_request;
  fillMetadata(&update_request);
  Message request;
  distributedWriteLock();  // avoid adding of new peers while inserting
  table->update(item);
  // at this point, update() has modified the revision such that all default
  // fields are also set, which allows remote peers to just patch the revision
  // into their table.
  update_request.set_serialized_revision(item->serializeUnderlying());
  request.impose<kUpdateRequest>(update_request);
  CHECK(peers_.undisputableBroadcast(&request));
  syncLatestCommitTime(*item);
  distributedUnlock();
}

void Chunk::attachTrigger(const std::function<void(const Id& id)>& callback) {
  std::lock_guard<std::mutex> lock(trigger_mutex_);
  trigger_ = callback;
}

void Chunk::bulkInsertLocked(const CRTable::NonConstRevisionMap& items,
                             const LogicalTime& time) {
  std::vector<proto::PatchRequest> insert_requests;
  insert_requests.resize(items.size());
  int i = 0;
  for (const CRTable::NonConstRevisionMap::value_type& item : items) {
    CHECK_NOTNULL(item.second.get());
    item.second->setChunkId(id());
    fillMetadata(&insert_requests[i]);
    ++i;
  }
  Message request;
  underlying_table_->bulkInsert(items, time);
  // at this point, insert() has modified the revisions such that all default
  // fields are also set, which allows remote peers to just patch the revision
  // into their table.
  i = 0;
  for (const CRTable::RevisionMap::value_type& item : items) {
    insert_requests[i]
        .set_serialized_revision(item.second->serializeUnderlying());
    request.impose<kInsertRequest>(insert_requests[i]);
    CHECK(peers_.undisputableBroadcast(&request));
    // TODO(tcies) also bulk this
    ++i;
  }
}

void Chunk::updateLocked(const LogicalTime& time,
                         const std::shared_ptr<Revision>& item) {
  CHECK(item != nullptr);
  CHECK(underlying_table_->type() == CRTable::Type::CRU);
  CRUTable* table = static_cast<CRUTable*>(underlying_table_);
  CHECK_EQ(id(), item->getChunkId());
  proto::PatchRequest update_request;
  fillMetadata(&update_request);
  Message request;
  table->update(item, time);
  // at this point, update() has modified the revision such that all default
  // fields are also set, which allows remote peers to just patch the revision
  // into their table.
  update_request.set_serialized_revision(item->serializeUnderlying());
  request.impose<kUpdateRequest>(update_request);
  CHECK(peers_.undisputableBroadcast(&request));
}

void Chunk::removeLocked(const LogicalTime& time,
                         const std::shared_ptr<Revision>& item) {
  CHECK(item != nullptr);
  CHECK(underlying_table_->type() == CRTable::Type::CRU);
  CRUTable* table = static_cast<CRUTable*>(underlying_table_);
  CHECK_EQ(item->getChunkId(), id());
  proto::PatchRequest remove_request;
  fillMetadata(&remove_request);
  Message request;
  table->remove(time, item);
  // at this point, update() has modified the revision such that all default
  // fields are also set, which allows remote peers to just patch the revision
  // into their table.
  remove_request.set_serialized_revision(item->serializeUnderlying());
  request.impose<kUpdateRequest>(remove_request);
  CHECK(peers_.undisputableBroadcast(&request));
}

bool Chunk::addPeer(const PeerId& peer) {
  std::lock_guard<std::mutex> add_peer_lock(add_peer_mutex_);
  {
    std::lock_guard<std::mutex> metalock(lock_.mutex);
    CHECK(isWriter(PeerId::self()));
  }
  Message request;
  if (peers_.peers().find(peer) != peers_.peers().end()) {
    LOG(FATAL) << "Peer already in swarm!";
    return false;
  }
  prepareInitRequest(&request);
  timing::Timer timer("init_request");
  if (!Hub::instance().ackRequest(peer, &request)) {
    timer.Stop();
    return false;
  }
  timer.Stop();
  // new peer is not ready to handle requests as the rest of the swarm. Still,
  // one last message is sent to the old swarm, notifying it of the new peer
  // and thus the new configuration:
  proto::NewPeerRequest new_peer_request;
  fillMetadata(&new_peer_request);
  new_peer_request.set_new_peer(peer.ipPort());
  request.impose<kNewPeerRequest>(new_peer_request);
  CHECK(peers_.undisputableBroadcast(&request));

  peers_.add(peer);
  return true;
}

size_t Chunk::addAllPeers() {
  size_t count = 0;
  std::lock_guard<std::mutex> add_peer_lock(add_peer_mutex_);
  {
    std::lock_guard<std::mutex> metalock(lock_.mutex);
    CHECK(isWriter(PeerId::self()));
  }
  Message request;
  proto::InitRequest init_request;
  fillMetadata(&init_request);
  initRequestSetData(&init_request);
  proto::NewPeerRequest new_peer_request;
  fillMetadata(&new_peer_request);

  std::set<PeerId> peers;
  Hub::instance().getPeers(&peers);

  for (const PeerId& peer : peers) {
    if (peers_.peers().find(peer) != peers_.peers().end()) {
      continue;
    }
    initRequestSetPeers(&init_request);
    request.impose<kInitRequest>(init_request);
    if (!Hub::instance().ackRequest(peer, &request)) {
      LOG(FATAL) << "Init request not accepted";
      continue;
    }
    new_peer_request.set_new_peer(peer.ipPort());
    request.impose<kNewPeerRequest>(new_peer_request);
    CHECK(peers_.undisputableBroadcast(&request));

    peers_.add(peer);
    ++count;
  }
  return count;
}

void Chunk::distributedReadLock() {
  if (log_locking_) {
    startState(READ_ATTEMPT);
  }
  timing::Timer timer("map_api::Chunk::distributedReadLock");
  std::unique_lock<std::mutex> metalock(lock_.mutex);
  if (isWriter(PeerId::self()) && lock_.thread == std::this_thread::get_id()) {
    // special case: also succeed. This is necessary e.g. when committing
    // transactions
    ++lock_.write_recursion_depth;
    metalock.unlock();
    timer.Discard();
    return;
  }
  while (lock_.state != DistributedRWLock::State::UNLOCKED &&
      lock_.state != DistributedRWLock::State::READ_LOCKED) {
    lock_.cv.wait(metalock);
  }
  CHECK(!relinquished_);
  lock_.state = DistributedRWLock::State::READ_LOCKED;
  ++lock_.n_readers;
  metalock.unlock();
  timer.Stop();
  if (log_locking_) {
    startState(READ_SUCCESS);
  }
}

void Chunk::distributedWriteLock() {
  if (log_locking_) {
    startState(WRITE_ATTEMPT);
  }
  timing::Timer timer("map_api::Chunk::distributedWriteLock");
  std::unique_lock<std::mutex> metalock(lock_.mutex);
  // case recursion TODO(tcies) abolish if possible
  if (isWriter(PeerId::self()) && lock_.thread == std::this_thread::get_id()) {
    ++lock_.write_recursion_depth;
    metalock.unlock();
    timer.Discard();
    return;
  }
  // case self, but other thread
  while (
      isWriter(PeerId::self()) && lock_.thread != std::this_thread::get_id()) {
    lock_.cv.wait(metalock);
  }
  while (true) {  // lock: attempt until success
    while (lock_.state != DistributedRWLock::State::UNLOCKED &&
           !(lock_.state == DistributedRWLock::State::ATTEMPTING &&
             lock_.thread == std::this_thread::get_id())) {
      lock_.cv.wait(metalock);
    }
    CHECK(!relinquished_);
    lock_.state = DistributedRWLock::State::ATTEMPTING;
    lock_.thread = std::this_thread::get_id();
    // unlocking metalock to avoid deadlocks when two peers try to acquire the
    // lock
    metalock.unlock();

    Message request, response;
    proto::ChunkRequestMetadata lock_request;
    fillMetadata(&lock_request);
    request.impose<kLockRequest>(lock_request);

    bool declined = false;
    if (FLAGS_writelock_persist) {
      if (peers_.peers().size()) {
        std::set<PeerId>::const_iterator it = peers_.peers().cbegin();
        Hub::instance().request(*it, &request, &response);
        if (response.isType<Message::kDecline>()) {
          declined = true;
        } else {
          ++it;
          for (; it != peers_.peers().cend(); ++it) {
            Hub::instance().request(*it, &request, &response);
            while (response.isType<Message::kDecline>()) {
              usleep(5000);  // TODO(tcies) flag?
              Hub::instance().request(*it, &request, &response);
            }
          }
        }
      }
    } else {
      for (const PeerId& peer : peers_.peers()) {
        Hub::instance().request(peer, &request, &response);
        if (response.isType<Message::kDecline>()) {
          // assuming no connection loss, a lock may only be declined by the
          // peer with lowest address
          declined = true;
          break;
        }
        // TODO(tcies) READ_LOCKED case - kReading & pulse - it would be
        // favorable for peers that have the lock read-locked to respond lest
        // they be considered disconnected due to timeout. A good solution
        // should be to have a custom response "reading, please stand by" with
        // lease & pulse to renew the reading lease.
        CHECK(response.isType<Message::kAck>());
        VLOG(3) << PeerId::self() << " got lock from " << peer;
      }
    }
    if (declined) {
      // if we fail to acquire the lock we return to "conditional wait if not
      // UNLOCKED or ATTEMPTING". Either the state has changed to "locked by
      // other" until then, or we will fail again.
      usleep(1000);
      metalock.lock();
      continue;
    }
    break;
  }
  // once all peers have accepted, the lock is considered acquired
  std::lock_guard<std::mutex> metalock_guard(lock_.mutex);
  CHECK(lock_.state == DistributedRWLock::State::ATTEMPTING);
  lock_.state = DistributedRWLock::State::WRITE_LOCKED;
  lock_.holder = PeerId::self();
  lock_.thread = std::this_thread::get_id();
  ++lock_.write_recursion_depth;
  timer.Stop();
  if (log_locking_) {
    startState(WRITE_SUCCESS);
  }
}

void Chunk::distributedUnlock() {
  std::unique_lock<std::mutex> metalock(lock_.mutex);
  switch (lock_.state) {
    case DistributedRWLock::State::UNLOCKED:
      LOG(FATAL) << "Attempted to unlock already unlocked lock";
      break;
    case DistributedRWLock::State::READ_LOCKED:
      if (!--lock_.n_readers) {
        lock_.state = DistributedRWLock::State::UNLOCKED;
        metalock.unlock();
        lock_.cv.notify_all();
        if (log_locking_) {
          startState(UNLOCKED);
        }
        return;
      }
      break;
    case DistributedRWLock::State::ATTEMPTING:
      LOG(FATAL) << "Can't abort lock request";
      break;
    case DistributedRWLock::State::WRITE_LOCKED:
      CHECK(lock_.holder == PeerId::self());
      CHECK(lock_.thread == std::this_thread::get_id());
      --lock_.write_recursion_depth;
      if (lock_.write_recursion_depth > 0) {
        metalock.unlock();
        return;
      }
      std::lock_guard<std::mutex> add_peer_lock(add_peer_mutex_);
      Message request, response;
      proto::ChunkRequestMetadata unlock_request;
      fillMetadata(&unlock_request);
      request.impose<kUnlockRequest, proto::ChunkRequestMetadata>(
          unlock_request);
      if (peers_.empty()) {
        lock_.state = DistributedRWLock::State::UNLOCKED;
      } else {
        bool self_unlocked = false;
        // NB peers can only change if someone else has locked the chunk
        const std::set<PeerId>& peers = peers_.peers();
        switch (static_cast<UnlockStrategy>(FLAGS_unlock_strategy)) {
          case REVERSE: {
            for (std::set<PeerId>::const_reverse_iterator rit = peers.rbegin();
                 rit != peers.rend(); ++rit) {
              if (!self_unlocked && *rit < PeerId::self()) {
                lock_.state = DistributedRWLock::State::UNLOCKED;
                self_unlocked = true;
              }
              Hub::instance().request(*rit, &request, &response);
              CHECK(response.isType<Message::kAck>());
              VLOG(3) << PeerId::self() << " released lock from " << *rit;
            }
            break;
          }
          case FORWARD: {
            CHECK(FLAGS_writelock_persist) << "forward unlock only works with "
                                              "writelock persist";
            for (const PeerId& peer : peers) {
              if (!self_unlocked && PeerId::self() < peer) {
                lock_.state = DistributedRWLock::State::UNLOCKED;
                self_unlocked = true;
              }
              Hub::instance().request(peer, &request, &response);
              CHECK(response.isType<Message::kAck>());
              VLOG(3) << PeerId::self() << " released lock from " << peer;
            }
            break;
          }
          case RANDOM: {
            CHECK(FLAGS_writelock_persist)
                << "Random doesn't work without writelock-persist";
            std::srand(LogicalTime::sample().serialize());
            std::vector<PeerId> mixed_peers(peers.cbegin(), peers.cend());
            std::random_shuffle(mixed_peers.begin(), mixed_peers.end());
            for (const PeerId& peer : mixed_peers) {
              Hub::instance().request(peer, &request, &response);
              CHECK(response.isType<Message::kAck>());
              VLOG(3) << PeerId::self() << " released lock from " << peer;
            }
            break;
          }
        }
        if (!self_unlocked) {
          // case we had the lowest address
          lock_.state = DistributedRWLock::State::UNLOCKED;
        }
      }
      metalock.unlock();
      lock_.cv.notify_all();
      if (log_locking_) {
        startState(UNLOCKED);
      }
      return;
  }
  metalock.unlock();
}

bool Chunk::isWriter(const PeerId& peer) {
  return (lock_.state == DistributedRWLock::State::WRITE_LOCKED &&
      lock_.holder == peer);
}

void Chunk::initRequestSetData(proto::InitRequest* request) {
  CHECK_NOTNULL(request);
  if (underlying_table_->type() == CRTable::Type::CR) {
    CRTable::RevisionMap data;
    underlying_table_->dumpChunk(id(), LogicalTime::sample(), &data);
    for (const CRTable::RevisionMap::value_type& data_pair : data) {
      request->add_serialized_items(data_pair.second->serializeUnderlying());
    }
  } else {
    CHECK(underlying_table_->type() == CRTable::Type::CRU);
    CRUTable::HistoryMap data;
    CRUTable* table = static_cast<CRUTable*>(underlying_table_);
    table->chunkHistory(id(), LogicalTime::sample(), &data);
    for (const CRUTable::HistoryMap::value_type& data_pair : data) {
      proto::History history_proto;
      for (const std::shared_ptr<const Revision>& revision : data_pair.second) {
        history_proto.mutable_revisions()->AddAllocated(
            new proto::Revision(*revision->underlying_revision_));
      }
      request->add_serialized_items(history_proto.SerializeAsString());
    }
  }
}

void Chunk::initRequestSetPeers(proto::InitRequest* request) {
  CHECK_NOTNULL(request);
  request->clear_peer_address();
  for (const PeerId& swarm_peer : peers_.peers()) {
    request->add_peer_address(swarm_peer.ipPort());
  }
  request->add_peer_address(PeerId::self().ipPort());
}

void Chunk::prepareInitRequest(Message* request) {
  CHECK_NOTNULL(request);
  proto::InitRequest init_request;
  fillMetadata(&init_request);
  initRequestSetPeers(&init_request);
  initRequestSetData(&init_request);
  request->impose<kInitRequest, proto::InitRequest>(init_request);
}

void Chunk::handleConnectRequest(const PeerId& peer, Message* response) {
  awaitInitialized();
  VLOG(3) << "Received connect request from " << peer;
  CHECK_NOTNULL(response);
  leave_lock_.readLock();
  if (relinquished_) {
    leave_lock_.unlock();
    response->decline();
    return;
  }
  /**
   * Connect request leads to adding a peer which requires locking, which
   * should NEVER block an RPC handler. This is because otherwise, if the lock
   * is locked, another peer will never succeed to unlock it because the
   * server thread of the RPC handler is busy.
   */
  std::thread handle_thread(handleConnectRequestThread, this, peer);
  handle_thread.detach();

  leave_lock_.unlock();
  response->ack();
}

void Chunk::handleConnectRequestThread(Chunk* self, const PeerId& peer) {
  self->awaitInitialized();
  CHECK_NOTNULL(self);
  self->leave_lock_.readLock();
  // the following is a special case which shall not be covered for now:
  CHECK(!self->relinquished_) <<
      "Peer left before it could handle a connect request";
  // probably the best way to solve it in the future is for the connect
  // requester to measure the pulse of the peer that promised to connect it
  // and retry connection with another peer if it dies before it connects the
  // peer
  self->distributedWriteLock();
  if (self->peers_.peers().find(peer) == self->peers_.peers().end()) {
    // Peer has no reason to refuse the init request.
    CHECK(self->addPeer(peer));
  } else {
    LOG(INFO) << "Peer requesting to join already in swarm, could have been "\
        "added by some requestParticipation() call.";
  }
  self->distributedUnlock();
  self->leave_lock_.unlock();
}

void Chunk::handleInsertRequest(const std::shared_ptr<Revision>& item,
                                Message* response) {
  CHECK(item != nullptr);
  CHECK_NOTNULL(response);
  awaitInitialized();
  leave_lock_.readLock();
  if (relinquished_) {
    leave_lock_.unlock();
    response->decline();
    return;
  }
  // an insert request may not happen while
  // another peer holds the write lock (i.e. inserts must be read-locked). Note
  // that this is not equivalent to checking state != WRITE_LOCKED, as the state
  // may be WRITE_LOCKED at some peers while in reality the lock is not write
  // locked:
  // A lock is only really WRITE_LOCKED when all peers agree that it is.
  // no further locking needed, elegantly
  {
    std::lock_guard<std::mutex> metalock(lock_.mutex);
    CHECK(!isWriter(PeerId::self()));
  }
  underlying_table_->patch(item);
  syncLatestCommitTime(*item);
  response->ack();
  leave_lock_.unlock();

  Id id = item->getId<Id>();  // TODO(tcies) what if leave during trigger?
  std::lock_guard<std::mutex> lock(trigger_mutex_);
  if (trigger_) {
    std::thread trigger_thread(trigger_, id);
    trigger_thread.detach();
  }
}

void Chunk::handleLeaveRequest(const PeerId& leaver, Message* response) {
  CHECK_NOTNULL(response);
  awaitInitialized();
  leave_lock_.readLock();
  CHECK(!relinquished_);  // sending a leave request to a disconnected peer
  // should be impossible by design
  std::lock_guard<std::mutex> metalock(lock_.mutex);
  CHECK(lock_.state == DistributedRWLock::State::WRITE_LOCKED);
  CHECK_EQ(lock_.holder, leaver);
  peers_.remove(leaver);
  leave_lock_.unlock();
  response->impose<Message::kAck>();
}

void Chunk::handleLockRequest(const PeerId& locker, Message* response) {
  CHECK_NOTNULL(response);
  awaitInitialized();
  leave_lock_.readLock();
  if (relinquished_) {
    // possible if two peer try to lock for leaving at the same time
    leave_lock_.unlock();
    response->decline();
    return;
  }
  // should be impossible by design
  std::unique_lock<std::mutex> metalock(lock_.mutex);
  // TODO(tcies) as mentioned before - respond immediately and pulse instead
  while (lock_.state == DistributedRWLock::State::READ_LOCKED) {
    lock_.cv.wait(metalock);
  }
  // preempted_state MUST NOT be set here, else it might be wrongly set to
  // write_locked if two peers contend for the same lock.
  switch (lock_.state) {
    case DistributedRWLock::State::UNLOCKED:
      lock_.preempted_state = DistributedRWLock::State::UNLOCKED;
      lock_.state = DistributedRWLock::State::WRITE_LOCKED;
      lock_.holder = locker;
      response->impose<Message::kAck>();
      break;
    case DistributedRWLock::State::READ_LOCKED:
      LOG(FATAL) << "This should never happen";
      break;
    case DistributedRWLock::State::ATTEMPTING:
      // special case: if address of requester is lower than self, may not
      // decline. If it is higher, it may decline only if we are the lowest
      // active peer.
      // This case occurs if two peers try to lock at the same time, and the
      // losing peer doesn't know that it's losing yet.
      if (PeerId::self() < *peers_.peers().begin()) {
        CHECK(PeerId::self() < locker);
        response->impose<Message::kDecline>();
      } else {
        // we DON'T need to roll back possible past requests. The current
        // situation can only happen if the requester has successfully achieved
        // the lock at all low-address peers, otherwise this situation couldn't
        // have occurred
        lock_.preempted_state = DistributedRWLock::State::ATTEMPTING;
        lock_.state = DistributedRWLock::State::WRITE_LOCKED;
        lock_.holder = locker;
        response->impose<Message::kAck>();
      }
      break;
    case DistributedRWLock::State::WRITE_LOCKED:
      response->impose<Message::kDecline>();
      break;
  }
  metalock.unlock();
  leave_lock_.unlock();
}

void Chunk::handleNewPeerRequest(const PeerId& peer, const PeerId& sender,
                                 Message* response) {
  CHECK_NOTNULL(response);
  awaitInitialized();
  leave_lock_.readLock();
  CHECK(!relinquished_);  // sending a new peer request to a disconnected peer
  // should be impossible by design
  std::lock_guard<std::mutex> metalock(lock_.mutex);
  CHECK(lock_.state == DistributedRWLock::State::WRITE_LOCKED);
  CHECK_EQ(lock_.holder, sender);
  peers_.add(peer);
  leave_lock_.unlock();
  response->impose<Message::kAck>();
}

void Chunk::handleUnlockRequest(const PeerId& locker, Message* response) {
  CHECK_NOTNULL(response);
  awaitInitialized();
  leave_lock_.readLock();
  CHECK(!relinquished_);  // sending a leave request to a disconnected peer
  // should be impossible by design
  std::unique_lock<std::mutex> metalock(lock_.mutex);
  CHECK(lock_.state == DistributedRWLock::State::WRITE_LOCKED);
  CHECK(lock_.holder == locker);
  CHECK(lock_.preempted_state == DistributedRWLock::State::UNLOCKED ||
        lock_.preempted_state == DistributedRWLock::State::ATTEMPTING);
  lock_.state = lock_.preempted_state;
  metalock.unlock();
  leave_lock_.unlock();
  lock_.cv.notify_all();
  response->impose<Message::kAck>();
}

void Chunk::handleUpdateRequest(const std::shared_ptr<Revision>& item,
                                const PeerId& sender, Message* response) {
  CHECK(item != nullptr);
  CHECK_NOTNULL(response);
  awaitInitialized();
  {
    std::lock_guard<std::mutex> metalock(lock_.mutex);
    CHECK(isWriter(sender));
  }
  CHECK(underlying_table_->type() == CRTable::Type::CRU);
  CRUTable* table = static_cast<CRUTable*>(underlying_table_);
  table->patch(item);
  syncLatestCommitTime(*item);
  response->ack();

  Id id = item->getId<Id>();  // TODO(tcies) what if leave during trigger?
  std::lock_guard<std::mutex> lock(trigger_mutex_);
  if (trigger_) {
    std::thread trigger_thread(trigger_, id);
    trigger_thread.detach();
  }
}

void Chunk::awaitInitialized() const {
  while (!initialized_) {
    usleep(1000);
  }
}

void Chunk::startState(LockState new_state) {
  // only log main thread
  if (std::this_thread::get_id() == main_thread_id_) {
    switch (new_state) {
      case LockState::UNLOCKED: {
        if (current_state_ == READ_SUCCESS || current_state_ == WRITE_SUCCESS) {
          logStateDuration(current_state_, current_state_start_,
                           std::chrono::system_clock::now());
          current_state_ = UNLOCKED;
        } else {
          LOG(FATAL) << "Invalid state transition: UL from " << current_state_;
        }
        break;
      }
      case LockState::READ_ATTEMPT:  // fallthrough intended
      case LockState::WRITE_ATTEMPT: {
        if (current_state_ == UNLOCKED) {
          current_state_start_ = std::chrono::system_clock::now();
          current_state_ = new_state;
        } else {
          LOG(FATAL) << "Invalid state transition: " << new_state << " from "
                     << current_state_;
        }
        break;
      }
      case LockState::READ_SUCCESS:  // fallthrough intended
      case LockState::WRITE_SUCCESS: {
        if (current_state_ == READ_ATTEMPT || current_state_ == WRITE_ATTEMPT) {
          logStateDuration(current_state_, current_state_start_,
                           std::chrono::system_clock::now());
          current_state_start_ = std::chrono::system_clock::now();
          current_state_ = new_state;
        } else {
          LOG(FATAL) << "Invalid state transition: S from " << current_state_;
        }
        break;
      }
    }
  }
}

void Chunk::logStateDuration(LockState state, const TimePoint& start,
                             const TimePoint& end) const {
  std::ofstream log_file(kLockSequenceFile, std::ios::out | std::ios::app);
  double d_start =
      static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(
          start - global_start_).count()) *
      kNanosecondsToSeconds;
  double d_end =
      static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(
          end - global_start_).count()) *
      kNanosecondsToSeconds;
  log_file << self_rank_ << " " << state << " " << d_start << " " << d_end
           << std::endl;
}
}  // namespace map_api
