#include "map-api/chunk.h"

#include "map-api/cru-table.h"
#include "map-api/net-table-manager.h"
#include "map-api/map-api-hub.h"
#include "core.pb.h"
#include "chunk.pb.h"

namespace map_api {

const char Chunk::kConnectRequest[] = "map_api_chunk_connect";
const char Chunk::kInitRequest[] = "map_api_chunk_init_request";
const char Chunk::kInsertRequest[] = "map_api_chunk_insert";
const char Chunk::kLeaveRequest[] = "map_api_chunk_leave_request";
const char Chunk::kLockRequest[] = "map_api_chunk_lock_request";
const char Chunk::kNewPeerRequest[] = "map_api_chunk_new_peer_request";
const char Chunk::kUnlockRequest[] = "map_api_chunk_unlock_request";
const char Chunk::kUpdateRequest[] = "map_api_chunk_update_request";

MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(
    Chunk::kConnectRequest, proto::ConnectRequest);
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(
    Chunk::kInitRequest, proto::InitRequest);
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(
    Chunk::kInsertRequest, proto::PatchRequest);
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(
    Chunk::kLeaveRequest, proto::ChunkRequestMetadata);
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(
    Chunk::kLockRequest, proto::ChunkRequestMetadata);
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(
    Chunk::kNewPeerRequest, proto::NewPeerRequest);
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(
    Chunk::kUnlockRequest, proto::ChunkRequestMetadata);
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(
    Chunk::kUpdateRequest, proto::PatchRequest);

bool Chunk::init(const Id& id, CRTable* underlying_table) {
  CHECK_NOTNULL(underlying_table);
  id_ = id;
  underlying_table_ = underlying_table;
  return true;
}

bool Chunk::init(
    const Id& id, const proto::InitRequest& init_request,
    CRTable* underlying_table) {
  CHECK(init(id, underlying_table));
  // connect to peers from connect_response TODO(tcies) notify of self
  CHECK_GT(init_request.peer_address_size(), 0);
  for (int i = 0; i < init_request.peer_address_size(); ++i) {
    peers_.add(PeerId(init_request.peer_address(i)));
  }
  // feed data from connect_response into underlying table TODO(tcies) piecewise
  for (int i = 0; i < init_request.serialized_revision_size(); ++i) {
    Revision data;
    CHECK(data.ParseFromString((init_request.serialized_revision(i))));
    CHECK(underlying_table->patch(data));
  }
  std::lock_guard<std::mutex> metalock(lock_.mutex);
  lock_.state = DistributedRWLock::State::WRITE_LOCKED;
  lock_.holder = PeerId(init_request.from_peer());
  return true;
}

bool Chunk::check(const ChunkTransaction& transaction) {
  {
    std::lock_guard<std::mutex> metalock(lock_.mutex);
    CHECK(isWriter(PeerId::self()));
  }
  for (const std::pair<const Id, std::shared_ptr<Revision> >& item :
      transaction.insertions_) {
    if (underlying_table_->getById(item.first, Time::now())) {
      LOG(WARNING) << "Table " << underlying_table_->name() <<
          " already contains id " << item.first;
      return false;
    }
  }
  CRUTable* table;
  if (!transaction.updates_.empty()) {
    CHECK(underlying_table_->type() == CRTable::Type::CRU);
    table = static_cast<CRUTable*>(underlying_table_);
  }
  for (const std::pair<const Id, std::shared_ptr<Revision> >& item :
      transaction.updates_) {
    Time latest_update;
    CHECK(table->getLatestUpdateTime(item.first, &latest_update));
    if (latest_update >= transaction.begin_time_) {
      return false;
    }
  }
  return true;
}

bool Chunk::commit(const ChunkTransaction& transaction) {
  distributedWriteLock();
  if (!check(transaction)) {
    distributedUnlock();
    return false;
  }
  for (const std::pair<const Id, std::shared_ptr<Revision> >& item :
      transaction.insertions_) {
    CHECK(insert(item.second.get()));
  }
  for (const std::pair<const Id, std::shared_ptr<Revision> >& item :
      transaction.updates_) {
    update(item.second.get());
  }
  distributedUnlock();
  return true;
}

Id Chunk::id() const {
  // TODO(tcies) implement
  return id_;
}

bool Chunk::insert(Revision* item) {
  CHECK_NOTNULL(item);
  item->set(NetTable::kChunkIdField, id());
  proto::PatchRequest insert_request;
  insert_request.set_table(underlying_table_->name());
  insert_request.set_chunk_id(id().hexString());
  insert_request.set_from_peer(PeerId::self().ipPort());
  Message request;
  distributedReadLock(); // avoid adding of new peers while inserting
  underlying_table_->insert(item);
  // at this point, insert() has modified the revision such that all default
  // fields are also set, which allows remote peers to just patch the revision
  // into their table.
  insert_request.set_serialized_revision(item->SerializeAsString());
  request.impose<kInsertRequest>(insert_request);
  CHECK(peers_.undisputableBroadcast(request));
  distributedUnlock();
  return true;
}

std::shared_ptr<ChunkTransaction> Chunk::newTransaction() {
    return std::shared_ptr<ChunkTransaction>(
        new ChunkTransaction(Time::now(), underlying_table_));
}
std::shared_ptr<ChunkTransaction> Chunk::newTransaction(const Time& time) {
  CHECK(time <= Time::now());
    return std::shared_ptr<ChunkTransaction>(
        new ChunkTransaction(time, underlying_table_));
}

int Chunk::peerSize() const {
  return peers_.size();
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
  CHECK(peers_.undisputableBroadcast(request));
  relinquished_ = true;
  leave_lock_.unlock();
  distributedUnlock(); // i.e. must be able to handle unlocks from outside
  // the swarm. Should this pose problems in the future, we could tie unlocking
  // to leaving.
}

void Chunk::lock() {
  distributedWriteLock();
}

int Chunk::requestParticipation() {
  int new_participant_count = 0;
  distributedWriteLock();
  std::set<PeerId> hub_peers;
  MapApiHub::instance().getPeers(&hub_peers);
  for (const PeerId& hub_peer : hub_peers) {
    if (peers_.peers().find(hub_peer) == peers_.peers().end()) {
      if (addPeer(hub_peer)) {
        ++new_participant_count;
      }
    }
  }
  distributedUnlock();
  return new_participant_count;
}

void Chunk::unlock() {
  distributedUnlock();
}

void Chunk::update(Revision* item) {
  CHECK_NOTNULL(item);
  CHECK(underlying_table_->type() == CRTable::Type::CRU);
  CRUTable* table = static_cast<CRUTable*>(underlying_table_);
  CHECK(item->verify(NetTable::kChunkIdField, id()));
  proto::PatchRequest update_request;
  update_request.set_table(underlying_table_->name());
  update_request.set_chunk_id(id().hexString());
  update_request.set_from_peer(PeerId::self().ipPort());
  Message request;
  distributedWriteLock(); // avoid adding of new peers while inserting
  table->update(item);
  // at this point, update() has modified the revision such that all default
  // fields are also set, which allows remote peers to just patch the revision
  // into their table.
  update_request.set_serialized_revision(item->SerializeAsString());
  request.impose<kUpdateRequest>(update_request);
  CHECK(peers_.undisputableBroadcast(request));
  distributedUnlock();
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
  if (!MapApiHub::instance().ackRequest(peer, request)) {
    return false;
  }
  // new peer is not ready to handle requests as the rest of the swarm. Still,
  // one last message is sent to the old swarm, notifying it of the new peer
  // and thus the new configuration:
  proto::NewPeerRequest new_peer_request;
  new_peer_request.set_table(underlying_table_->name());
  new_peer_request.set_chunk_id(id().hexString());
  new_peer_request.set_new_peer(peer.ipPort());
  new_peer_request.set_from_peer(PeerId::self().ipPort());
  request.impose<kNewPeerRequest>(new_peer_request);
  CHECK(peers_.undisputableBroadcast(request));

  peers_.add(peer);
  return true;
}

void Chunk::distributedReadLock() {
  std::unique_lock<std::mutex> metalock(lock_.mutex);
  if (isWriter(PeerId::self()) && lock_.thread == std::this_thread::get_id()) {
    // special case: also succeed. This is necessary e.g. when committing
    // transactions
    ++lock_.write_recursion_depth;
    metalock.unlock();
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
}

void Chunk::distributedWriteLock() {
  std::unique_lock<std::mutex> metalock(lock_.mutex);
  if (isWriter(PeerId::self()) && lock_.thread == std::this_thread::get_id()) {
    ++lock_.write_recursion_depth;
    metalock.unlock();
    return;
  }
  while(true) { // lock: attempt until success
    while (lock_.state != DistributedRWLock::State::UNLOCKED &&
        lock_.state != DistributedRWLock::State::ATTEMPTING) {
      lock_.cv.wait(metalock);
    }
    CHECK(!relinquished_);
    lock_.state = DistributedRWLock::State::ATTEMPTING;
    // unlocking metalock to avoid deadlocks when two peers try to acquire the
    // lock
    metalock.unlock();

    Message request, response;
    proto::ChunkRequestMetadata lock_request;
    fillMetadata(&lock_request);
    request.impose<kLockRequest>(lock_request);

    bool declined = false;
    for (const PeerId& peer : peers_.peers()) {
      MapApiHub::instance().request(peer, request, &response);
      if (response.isType<Message::kDecline>()) {
        // assuming no connection loss, a lock may only be declined by the peer
        // with lowest address
        declined = true;
        break;
      }
      // TODO(tcies) READ_LOCKED case - kReading & pulse - it would be favorable
      // for peers that have the lock read-locked to respond lest they be
      // considered disconnected due to timeout. A good solution should be to
      // have a custom response "reading, please stand by" with lease & pulse to
      // renew the reading lease.
      CHECK(response.isType<Message::kAck>());
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
      // to make sure that possibly concurrent locking works correctly, we
      // need to unlock in reverse order of locking, i.e. we must ensure that
      // if peer with address A considers the lock unlocked, any peer B > A
      // (including the local one) does as well
      if (peers_.empty()) {
        lock_.state = DistributedRWLock::State::UNLOCKED;
      }
      else {
        bool self_unlocked = false;
        for (std::set<PeerId>::const_reverse_iterator rit =
            peers_.peers().rbegin(); rit != peers_.peers().rend(); ++rit) {
          if (!self_unlocked && *rit < PeerId::self()) {
            lock_.state = DistributedRWLock::State::UNLOCKED;
            self_unlocked = true;
          }
          MapApiHub::instance().request(*rit, request, &response);
          CHECK(response.isType<Message::kAck>());
        }
        if (!self_unlocked) {
          // case we had the lowest address
          lock_.state = DistributedRWLock::State::UNLOCKED;
        }
      }
      metalock.unlock();
      lock_.cv.notify_all();
      return;
  }
  metalock.unlock();
}

void Chunk::fillMetadata(proto::ChunkRequestMetadata* destination) {
  CHECK_NOTNULL(destination);
  destination->set_table(underlying_table_->name());
  destination->set_chunk_id(id().hexString());
  destination->set_from_peer(PeerId::self().ipPort());
}

bool Chunk::isWriter(const PeerId& peer) {
  return (lock_.state == DistributedRWLock::State::WRITE_LOCKED &&
      lock_.holder == peer);
}

void Chunk::prepareInitRequest(Message* request) {
  CHECK_NOTNULL(request);
  proto::InitRequest init_request;

  init_request.set_table(underlying_table_->name());
  init_request.set_chunk_id(id().hexString());
  init_request.set_from_peer(PeerId::self().ipPort());

  for (const PeerId& swarm_peer : peers_.peers()) {
    init_request.add_peer_address(swarm_peer.ipPort());
  }
  init_request.add_peer_address(PeerId::self().ipPort());

  std::unordered_map<Id, std::shared_ptr<Revision> > data;
  underlying_table_->find(NetTable::kChunkIdField, id(), Time::now(), &data);
  for (const std::pair<const Id, std::shared_ptr<Revision> >& data_pair :
      data) {
    init_request.add_serialized_revision(
        data_pair.second->SerializeAsString());
  }

  request->impose<kInitRequest, proto::InitRequest>(init_request);
}

void Chunk::handleConnectRequest(const PeerId& peer, Message* response) {
  LOG(INFO) << "Received connect request from " << peer;
  CHECK_NOTNULL(response);
  leave_lock_.readLock();
  if (relinquished_) {
    leave_lock_.unlock();
    response->decline();
    return;
  }

  distributedWriteLock();
  if (peers_.peers().find(peer) == peers_.peers().end()) {
    CHECK(addPeer(peer)); // peer has no reason to refuse the init request
  } else {
    LOG(INFO) << "Peer requesting to join already in swarm, could have been "\
        "added by some requestParticipation() call.";
  }
  distributedUnlock();

  leave_lock_.unlock();
  response->ack();
}

void Chunk::handleInsertRequest(const Revision& item, Message* response) {
  CHECK_NOTNULL(response);
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
  response->ack();
  leave_lock_.unlock();
}

void Chunk::handleLeaveRequest(const PeerId& leaver, Message* response) {
  CHECK_NOTNULL(response);
  leave_lock_.readLock();
  CHECK(!relinquished_); // sending a leave request to a disconnected peer
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
  leave_lock_.readLock();
  if(relinquished_) {
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
  switch (lock_.state) {
    case DistributedRWLock::State::UNLOCKED:
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
      }
      else {
        // we DON'T need to roll back possible past requests. The current
        // situation can only happen if the requester has successfully achieved
        // the lock at all low-address peers, otherwise this situation couldn't
        // have occurred
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
  leave_lock_.readLock();
  CHECK(!relinquished_); // sending a new peer request to a disconnected peer
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
  leave_lock_.readLock();
  CHECK(!relinquished_); // sending a leave request to a disconnected peer
  // should be impossible by design
  std::unique_lock<std::mutex> metalock(lock_.mutex);
  CHECK(lock_.state == DistributedRWLock::State::WRITE_LOCKED);
  CHECK(lock_.holder == locker);
  lock_.state = DistributedRWLock::State::UNLOCKED;
  metalock.unlock();
  leave_lock_.unlock();
  lock_.cv.notify_all();
  response->impose<Message::kAck>();
}

void Chunk::handleUpdateRequest(const Revision& item, const PeerId& sender,
                                Message* response) {
  CHECK_NOTNULL(response);
  {
    std::lock_guard<std::mutex> metalock(lock_.mutex);
    CHECK(isWriter(sender));
  }
  CHECK(underlying_table_->type() == CRTable::Type::CRU);
  CRUTable* table = static_cast<CRUTable*>(underlying_table_);
  table->patch(item);
  Id id;
  Time current, updated;
  item.get(CRTable::kIdField, &id);
  item.get(CRUTable::kPreviousTimeField, &current);
  item.get(CRUTable::kUpdateTimeField, &updated);
  table->updateCurrentReferToUpdatedCRUDerived(id, current, updated);
  response->ack();
}

} // namespace map_api
