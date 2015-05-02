#include <map-api/legacy-chunk.h>
#include <fstream>  // NOLINT
#include <unordered_set>

#include <multiagent-mapping-common/backtrace.h>
#include <multiagent-mapping-common/conversions.h>
#include <timing/timer.h>

#include "./core.pb.h"
#include "./chunk.pb.h"
#include "../include/map-api/legacy-chunk-data-ram-container.h"
#include "../include/map-api/legacy-chunk-data-stxxl-container.h"
#include "map-api/hub.h"
#include "map-api/message.h"
#include "map-api/net-table-manager.h"
#include "map-api/revision-map.h"

DEFINE_bool(use_external_memory, false, "STXXL vs. RAM data container.");
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
DEFINE_bool(map_api_time_chunk, false, "Toggle chunk timing.");

DECLARE_bool(blame_trigger);

namespace map_api {

const char LegacyChunk::kConnectRequest[] = "map_api_chunk_connect";
const char LegacyChunk::kInitRequest[] = "map_api_chunk_init_request";
const char LegacyChunk::kInsertRequest[] = "map_api_chunk_insert";
const char LegacyChunk::kLeaveRequest[] = "map_api_chunk_leave_request";
const char LegacyChunk::kLockRequest[] = "map_api_chunk_lock_request";
const char LegacyChunk::kNewPeerRequest[] = "map_api_chunk_new_peer_request";
const char LegacyChunk::kUnlockRequest[] = "map_api_chunk_unlock_request";
const char LegacyChunk::kUpdateRequest[] = "map_api_chunk_update_request";

MAP_API_PROTO_MESSAGE(LegacyChunk::kConnectRequest,
                      proto::ChunkRequestMetadata);
MAP_API_PROTO_MESSAGE(LegacyChunk::kInitRequest, proto::InitRequest);
MAP_API_PROTO_MESSAGE(LegacyChunk::kInsertRequest, proto::PatchRequest);
MAP_API_PROTO_MESSAGE(LegacyChunk::kLeaveRequest, proto::ChunkRequestMetadata);
MAP_API_PROTO_MESSAGE(LegacyChunk::kLockRequest, proto::ChunkRequestMetadata);
MAP_API_PROTO_MESSAGE(LegacyChunk::kNewPeerRequest, proto::NewPeerRequest);
MAP_API_PROTO_MESSAGE(LegacyChunk::kUnlockRequest, proto::ChunkRequestMetadata);
MAP_API_PROTO_MESSAGE(LegacyChunk::kUpdateRequest, proto::PatchRequest);

const char LegacyChunk::kLockSequenceFile[] = "meas_lock_sequence.txt";

template <>
void LegacyChunk::fillMetadata<proto::ChunkRequestMetadata>(
    proto::ChunkRequestMetadata* destination) const {
  CHECK_NOTNULL(destination)->set_table(data_container_->name());
  id().serialize(destination->mutable_chunk_id());
}

LegacyChunk::~LegacyChunk() {}

bool LegacyChunk::init(const common::Id& id,
                       std::shared_ptr<TableDescriptor> descriptor,
                       bool initialize) {
  CHECK(descriptor);
  id_ = id;
  if (FLAGS_use_external_memory) {
    data_container_.reset(new LegacyChunkDataStxxlContainer);
  } else {
    data_container_.reset(new LegacyChunkDataRamContainer);
  }
  CHECK(data_container_->init(descriptor));
  if (initialize) {
    initialized_.notify();
  }
  return true;
}

void LegacyChunk::initializeNewImpl(
    const common::Id& id, const std::shared_ptr<TableDescriptor>& descriptor) {
  CHECK(init(id, descriptor, true));
}

bool LegacyChunk::init(const common::Id& id,
                       const proto::InitRequest& init_request,
                       const PeerId& sender,
                       std::shared_ptr<TableDescriptor> descriptor) {
  CHECK(init(id, descriptor, false));
  CHECK_GT(init_request.peer_address_size(), 0);
  for (int i = 0; i < init_request.peer_address_size(); ++i) {
    peers_.add(PeerId(init_request.peer_address(i)));
  }
  // feed data from connect_response into underlying table TODO(tcies) piecewise
  for (int i = 0; i < init_request.serialized_items_size(); ++i) {
    proto::History history_proto;
    CHECK(history_proto.ParseFromString(init_request.serialized_items(i)));
    CHECK_GT(history_proto.revisions_size(), 0);
    while (history_proto.revisions_size() > 0) {
      // using ReleaseLast allows zero-copy ownership transfer to the revision
      // object.
      std::shared_ptr<Revision> data =
          Revision::fromProto(std::unique_ptr<proto::Revision>(
              history_proto.mutable_revisions()->ReleaseLast()));
      CHECK(static_cast<LegacyChunkDataContainerBase*>(data_container_.get())
                ->patch(data));
      // TODO(tcies) guarantee order, then only sync latest time
      syncLatestCommitTime(*data);
    }
  }
  std::lock_guard<std::mutex> metalock(lock_.mutex);
  lock_.preempted_state = DistributedRWLock::State::UNLOCKED;
  lock_.state = DistributedRWLock::State::WRITE_LOCKED;
  lock_.holder = sender;
  initialized_.notify();
  // Because it would be wasteful to iterate over all entries to find the
  // actual latest time:
  return true;
}

void LegacyChunk::dumpItems(const LogicalTime& time,
                            ConstRevisionMap* items) const {
  CHECK_NOTNULL(items);
  distributedReadLock();
  data_container_->dump(time, items);
  distributedUnlock();
}

size_t LegacyChunk::numItems(const LogicalTime& time) const {
  distributedReadLock();
  size_t result = data_container_->numAvailableIds(time);
  distributedUnlock();
  return result;
}

size_t LegacyChunk::itemsSizeBytes(const LogicalTime& time) const {
  ConstRevisionMap items;
  distributedReadLock();
  data_container_->dump(time, &items);
  distributedUnlock();
  size_t num_bytes = 0;
  for (const std::pair<common::Id, std::shared_ptr<const Revision> >& item :
       items) {
    CHECK(item.second != nullptr);
    const Revision& revision = *item.second;
    num_bytes += revision.byteSize();
  }
  return num_bytes;
}

// TODO(tcies) cache? : Store commit times with chunks as commits occur,
// share this info consistently.
void LegacyChunk::getCommitTimes(const LogicalTime& sample_time,
                                 std::set<LogicalTime>* commit_times) const {
  CHECK_NOTNULL(commit_times);
  //  Using a temporary unordered map because it should have a faster insertion
  //  time. The expected amount of commit times << the expected amount of items,
  //  so this should be worth it.
  std::unordered_set<LogicalTime> unordered_commit_times;
  ConstRevisionMap items;
  LegacyChunkDataContainerBase::HistoryMap histories;
  distributedReadLock();
  static_cast<LegacyChunkDataContainerBase*>(data_container_.get())
      ->chunkHistory(id(), sample_time, &histories);
  distributedUnlock();
  for (const LegacyChunkDataContainerBase::HistoryMap::value_type& history :
       histories) {
    for (const std::shared_ptr<const Revision>& revision : history.second) {
      unordered_commit_times.insert(revision->getUpdateTime());
    }
  }
  commit_times->insert(unordered_commit_times.begin(),
                       unordered_commit_times.end());
}

bool LegacyChunk::insert(const LogicalTime& time,
                         const std::shared_ptr<Revision>& item) {
  CHECK(item != nullptr);
  item->setChunkId(id());
  proto::PatchRequest insert_request;
  fillMetadata(&insert_request);
  Message request;
  distributedReadLock();  // avoid adding of new peers while inserting
  static_cast<LegacyChunkDataContainerBase*>(data_container_.get())
      ->insert(time, item);
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

int LegacyChunk::peerSize() const { return peers_.size(); }

void LegacyChunk::enableLockLogging() {
  log_locking_ = true;
  self_rank_ = PeerId::selfRank();
  std::ofstream file(kLockSequenceFile, std::ios::out | std::ios::trunc);
  global_start_ = std::chrono::system_clock::now();
  current_state_ = UNLOCKED;
  main_thread_id_ = std::this_thread::get_id();
}

void LegacyChunk::leaveImpl() {
  Message request;
  proto::ChunkRequestMetadata metadata;
  fillMetadata(&metadata);
  request.impose<kLeaveRequest>(metadata);
  distributedWriteLock();
  // leaving must be atomic wrt request handlers to prevent conflicts
  // this must happen after acquring the write lock to avoid deadlocks, should
  // two peers try to leave at the same time.
  {
    common::ScopedWriteLock lock(&leave_lock_);
    CHECK(peers_.undisputableBroadcast(&request));
    relinquished_ = true;
  }
  distributedUnlock();  // i.e. must be able to handle unlocks from outside
  // the swarm. Should this pose problems in the future, we could tie unlocking
  // to leaving.
}

void LegacyChunk::awaitShared() {
  peers_.awaitNonEmpty("Waiting for chunk " + id().hexString() + " of table " +
                       data_container_->name() + " to be shared...");
}

void LegacyChunk::writeLock() { distributedWriteLock(); }

void LegacyChunk::readLock() const { distributedReadLock(); }

bool LegacyChunk::isWriteLocked() {
  std::lock_guard<std::mutex> metalock(lock_.mutex);
  return isWriter(PeerId::self()) && lock_.thread == std::this_thread::get_id();
}

void LegacyChunk::unlock() const { distributedUnlock(); }

// not expressing in terms of the peer-specifying overload in order to avoid
// unnecessary distributed lock and unlocks
int LegacyChunk::requestParticipation() {
  distributedWriteLock();
  size_t new_participant_count = addAllPeers();
  distributedUnlock();
  return new_participant_count;
}

int LegacyChunk::requestParticipation(const PeerId& peer) {
  if (!Hub::instance().hasPeer(peer)) {
    return 0;
  }
  int participant_count = 0;
  distributedWriteLock();
  std::set<PeerId> hub_peers;
  Hub::instance().getPeers(&hub_peers);
  if (peers_.peers().find(peer) == peers_.peers().end()) {
    if (addPeer(peer)) {
      ++participant_count;
    }
  } else {
    VLOG(3) << "Peer " << peer << " already in swarm!";
    ++participant_count;
  }
  distributedUnlock();
  return participant_count;
}

void LegacyChunk::update(const std::shared_ptr<Revision>& item) {
  CHECK(item != nullptr);
  CHECK_EQ(id(), item->getChunkId());
  proto::PatchRequest update_request;
  fillMetadata(&update_request);
  Message request;
  distributedWriteLock();  // avoid adding of new peers while inserting
  static_cast<LegacyChunkDataContainerBase*>(data_container_.get())
      ->update(LogicalTime::sample(), item);
  // at this point, update() has modified the revision such that all default
  // fields are also set, which allows remote peers to just patch the revision
  // into their table.
  update_request.set_serialized_revision(item->serializeUnderlying());
  request.impose<kUpdateRequest>(update_request);
  CHECK(peers_.undisputableBroadcast(&request));
  syncLatestCommitTime(*item);
  distributedUnlock();
}

LogicalTime LegacyChunk::getLatestCommitTime() const {
  distributedReadLock();
  LogicalTime result = latest_commit_time_;
  distributedUnlock();
  return result;
}

void LegacyChunk::bulkInsertLocked(const MutableRevisionMap& items,
                                   const LogicalTime& time) {
  std::vector<proto::PatchRequest> insert_requests;
  insert_requests.resize(items.size());
  int i = 0;
  for (const MutableRevisionMap::value_type& item : items) {
    CHECK_NOTNULL(item.second.get());
    item.second->setChunkId(id());
    fillMetadata(&insert_requests[i]);
    ++i;
  }
  Message request;
  static_cast<LegacyChunkDataContainerBase*>(data_container_.get())
      ->bulkInsert(time, items);
  // at this point, insert() has modified the revisions such that all default
  // fields are also set, which allows remote peers to just patch the revision
  // into their table.
  i = 0;
  for (const ConstRevisionMap::value_type& item : items) {
    insert_requests[i]
        .set_serialized_revision(item.second->serializeUnderlying());
    request.impose<kInsertRequest>(insert_requests[i]);
    CHECK(peers_.undisputableBroadcast(&request));
    // TODO(tcies) also bulk this
    ++i;
  }
}

void LegacyChunk::updateLocked(const LogicalTime& time,
                               const std::shared_ptr<Revision>& item) {
  CHECK(item != nullptr);
  CHECK_EQ(id(), item->getChunkId());
  proto::PatchRequest update_request;
  fillMetadata(&update_request);
  Message request;
  static_cast<LegacyChunkDataContainerBase*>(data_container_.get())
      ->update(time, item);
  // at this point, update() has modified the revision such that all default
  // fields are also set, which allows remote peers to just patch the revision
  // into their table.
  update_request.set_serialized_revision(item->serializeUnderlying());
  request.impose<kUpdateRequest>(update_request);
  CHECK(peers_.undisputableBroadcast(&request));
}

void LegacyChunk::removeLocked(const LogicalTime& time,
                               const std::shared_ptr<Revision>& item) {
  CHECK(item != nullptr);
  CHECK_EQ(item->getChunkId(), id());
  proto::PatchRequest remove_request;
  fillMetadata(&remove_request);
  Message request;
  static_cast<LegacyChunkDataContainerBase*>(data_container_.get())
      ->remove(time, item);
  // at this point, update() has modified the revision such that all default
  // fields are also set, which allows remote peers to just patch the revision
  // into their table.
  remove_request.set_serialized_revision(item->serializeUnderlying());
  request.impose<kUpdateRequest>(remove_request);
  CHECK(peers_.undisputableBroadcast(&request));
}

bool LegacyChunk::addPeer(const PeerId& peer) {
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
    LOG(WARNING) << peer << " did not accept init request!";
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

size_t LegacyChunk::addAllPeers() {
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

void LegacyChunk::distributedReadLock() const {
  if (log_locking_) {
    startState(READ_ATTEMPT);
  }
  timing::Timer timer("map_api::Chunk::distributedReadLock",
                      !FLAGS_map_api_time_chunk);
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

void LegacyChunk::distributedWriteLock() {
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
  while (isWriter(PeerId::self()) &&
         lock_.thread != std::this_thread::get_id()) {
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

void LegacyChunk::distributedUnlock() const {
  std::unique_lock<std::mutex> metalock(lock_.mutex);
  switch (lock_.state) {
    case DistributedRWLock::State::UNLOCKED: {
      LOG(FATAL) << "Attempted to unlock already unlocked lock";
      break;
    }
    case DistributedRWLock::State::READ_LOCKED: {
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
    }
    case DistributedRWLock::State::ATTEMPTING: {
      LOG(FATAL) << "Can't abort lock request";
      break;
    }
    case DistributedRWLock::State::WRITE_LOCKED: {
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
        if (FLAGS_blame_trigger) {
          LOG(WARNING) << "Unlock from here may cause triggers for " << id();
          LOG(INFO) << common::backtrace();
        }
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
              VLOG(4) << PeerId::self() << " released lock from " << *rit;
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
              VLOG(4) << PeerId::self() << " released lock from " << peer;
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
              VLOG(4) << PeerId::self() << " released lock from " << peer;
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
  }
  metalock.unlock();
}

bool LegacyChunk::isWriter(const PeerId& peer) const {
  return (lock_.state == DistributedRWLock::State::WRITE_LOCKED &&
          lock_.holder == peer);
}

void LegacyChunk::initRequestSetData(proto::InitRequest* request) {
  CHECK_NOTNULL(request);
  LegacyChunkDataContainerBase::HistoryMap data;
  static_cast<LegacyChunkDataContainerBase*>(data_container_.get())
      ->chunkHistory(id(), LogicalTime::sample(), &data);
  for (const LegacyChunkDataContainerBase::HistoryMap::value_type& data_pair :
       data) {
    proto::History history_proto;
    for (const std::shared_ptr<const Revision>& revision : data_pair.second) {
      history_proto.mutable_revisions()->AddAllocated(
          new proto::Revision(*revision->underlying_revision_));
    }
    request->add_serialized_items(history_proto.SerializeAsString());
  }
}

void LegacyChunk::initRequestSetPeers(proto::InitRequest* request) {
  CHECK_NOTNULL(request);
  request->clear_peer_address();
  for (const PeerId& swarm_peer : peers_.peers()) {
    request->add_peer_address(swarm_peer.ipPort());
  }
  request->add_peer_address(PeerId::self().ipPort());
}

void LegacyChunk::prepareInitRequest(Message* request) {
  CHECK_NOTNULL(request);
  proto::InitRequest init_request;
  fillMetadata(&init_request);
  initRequestSetPeers(&init_request);
  initRequestSetData(&init_request);
  request->impose<kInitRequest, proto::InitRequest>(init_request);
}

void LegacyChunk::handleConnectRequest(const PeerId& peer, Message* response) {
  awaitInitialized();
  VLOG(3) << "Received connect request from " << peer;
  CHECK_NOTNULL(response);
  leave_lock_.acquireReadLock();
  if (relinquished_) {
    leave_lock_.releaseReadLock();
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

  leave_lock_.releaseReadLock();
  response->ack();
}

void LegacyChunk::handleConnectRequestThread(LegacyChunk* self,
                                             const PeerId& peer) {
  self->awaitInitialized();
  CHECK_NOTNULL(self);
  self->leave_lock_.acquireReadLock();
  // the following is a special case which shall not be covered for now:
  CHECK(!self->relinquished_)
      << "Peer left before it could handle a connect request";
  // probably the best way to solve it in the future is for the connect
  // requester to measure the pulse of the peer that promised to connect it
  // and retry connection with another peer if it dies before it connects the
  // peer
  self->distributedWriteLock();
  if (self->peers_.peers().find(peer) == self->peers_.peers().end()) {
    // Peer has no reason to refuse the init request.
    CHECK(self->addPeer(peer));
  } else {
    LOG(INFO) << "Peer requesting to join already in swarm, could have been "
                 "added by some requestParticipation() call.";
  }
  self->distributedUnlock();
  self->leave_lock_.releaseReadLock();
}

void LegacyChunk::handleInsertRequest(const std::shared_ptr<Revision>& item,
                                      Message* response) {
  CHECK(item != nullptr);
  CHECK_NOTNULL(response);
  awaitInitialized();
  leave_lock_.acquireReadLock();
  if (relinquished_) {
    leave_lock_.releaseReadLock();
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
  static_cast<LegacyChunkDataContainerBase*>(data_container_.get())
      ->patch(item);
  syncLatestCommitTime(*item);
  response->ack();
  leave_lock_.releaseReadLock();

  // TODO(tcies) what if leave during trigger?
  common::Id id = item->getId<common::Id>();
  handleCommitInsert(id);
}

void LegacyChunk::handleLeaveRequest(const PeerId& leaver, Message* response) {
  CHECK_NOTNULL(response);
  awaitInitialized();
  leave_lock_.acquireReadLock();
  CHECK(!relinquished_);  // sending a leave request to a disconnected peer
  // should be impossible by design
  std::lock_guard<std::mutex> metalock(lock_.mutex);
  CHECK(lock_.state == DistributedRWLock::State::WRITE_LOCKED);
  CHECK_EQ(lock_.holder, leaver);
  peers_.remove(leaver);
  leave_lock_.releaseReadLock();
  response->impose<Message::kAck>();
}

void LegacyChunk::handleLockRequest(const PeerId& locker, Message* response) {
  CHECK_NOTNULL(response);
  awaitInitialized();
  leave_lock_.acquireReadLock();
  if (relinquished_) {
    // possible if two peer try to lock for leaving at the same time
    leave_lock_.releaseReadLock();
    response->decline();
    return;
  }
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
  leave_lock_.releaseReadLock();
}

void LegacyChunk::handleNewPeerRequest(const PeerId& peer, const PeerId& sender,
                                       Message* response) {
  CHECK_NOTNULL(response);
  awaitInitialized();
  leave_lock_.acquireReadLock();
  CHECK(!relinquished_);  // sending a new peer request to a disconnected peer
  // should be impossible by design
  std::lock_guard<std::mutex> metalock(lock_.mutex);
  CHECK(lock_.state == DistributedRWLock::State::WRITE_LOCKED);
  CHECK_EQ(lock_.holder, sender);
  peers_.add(peer);
  leave_lock_.releaseReadLock();
  response->impose<Message::kAck>();
}

void LegacyChunk::handleUnlockRequest(const PeerId& locker, Message* response) {
  CHECK_NOTNULL(response);
  awaitInitialized();
  leave_lock_.acquireReadLock();
  CHECK(!relinquished_);  // sending a leave request to a disconnected peer
  // should be impossible by design
  std::unique_lock<std::mutex> metalock(lock_.mutex);
  CHECK(lock_.state == DistributedRWLock::State::WRITE_LOCKED);
  CHECK(lock_.holder == locker);
  CHECK(lock_.preempted_state == DistributedRWLock::State::UNLOCKED ||
        lock_.preempted_state == DistributedRWLock::State::ATTEMPTING);
  lock_.state = lock_.preempted_state;
  metalock.unlock();
  leave_lock_.releaseReadLock();
  lock_.cv.notify_all();
  response->impose<Message::kAck>();
  handleCommitEnd();
}

void LegacyChunk::handleUpdateRequest(const std::shared_ptr<Revision>& item,
                                      const PeerId& sender, Message* response) {
  CHECK(item != nullptr);
  CHECK_NOTNULL(response);
  awaitInitialized();
  {
    std::lock_guard<std::mutex> metalock(lock_.mutex);
    CHECK(isWriter(sender));
  }
  static_cast<LegacyChunkDataContainerBase*>(data_container_.get())
      ->patch(item);
  syncLatestCommitTime(*item);
  response->ack();

  // TODO(tcies) what if leave during trigger?
  common::Id id = item->getId<common::Id>();
  handleCommitUpdate(id);
}

void LegacyChunk::awaitInitialized() const { initialized_.wait(); }

void LegacyChunk::startState(LockState new_state) const {
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

void LegacyChunk::logStateDuration(LockState state, const TimePoint& start,
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
