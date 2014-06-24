#include "map-api/chunk.h"

#include "map-api/cru-table.h"
#include "map-api/net-table-manager.h"
#include "map-api/map-api-hub.h"
#include "core.pb.h"
#include "chunk.pb.h"

DECLARE_string(ip_port);

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

Id Chunk::id() const {
  // TODO(tcies) implement
  return id_;
}

bool Chunk::insert(Revision* item) {
  CHECK_NOTNULL(item);
  item->set(NetCRTable::kChunkIdField, id());
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

int Chunk::peerSize() const {
  return peers_.size();
}

void Chunk::leave() {
  LOG(INFO) << PeerId::self() << " invoked leave";
  Message request;
  proto::ChunkRequestMetadata metadata;
  fillMetadata(&metadata);
  request.impose<kLeaveRequest>(metadata);
  distributedWriteLock();
  CHECK(peers_.undisputableBroadcast(request));
  relinquished_ = true;
  distributedUnlock(); // i.e. must be able to handle unlocks from outside
  // the swarm. Slightly unclean design maybe but IMO not too disturbing
  LOG(INFO) << PeerId::self() << " left chunk " << id();
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

bool Chunk::update(Revision* item) {
  CHECK_NOTNULL(item);
  CRUTable* table = dynamic_cast<CRUTable*>(underlying_table_);
  CHECK(table);
  CHECK(item->verify(NetCRTable::kChunkIdField, id()));
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
  return true;
}

bool Chunk::addPeer(const PeerId& peer) {
  std::lock_guard<std::mutex> add_peer_lock(add_peer_mutex_);
  CHECK(isWriter());
  Message request;
  if (peers_.peers().find(peer) != peers_.peers().end()) {
    LOG(WARNING) << "Peer already in swarm!";
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
  new_peer_request.set_from_peer(FLAGS_ip_port);
  request.impose<kNewPeerRequest>(new_peer_request);
  CHECK(peers_.undisputableBroadcast(request));
  // add peer
  peers_.add(peer);
  return true;
}

void Chunk::distributedReadLock() {
  std::unique_lock<std::mutex> metalock(lock_.mutex);
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
  while(true) { // lock: attempt until success
    std::unique_lock<std::mutex> metalock(lock_.mutex);
    while (lock_.state != DistributedRWLock::State::UNLOCKED &&
        lock_.state != DistributedRWLock::State::ATTEMPTING) {
      lock_.cv.wait(metalock);
    }
    CHECK(!relinquished_); // TODO(tcies) might actually happen when receiving
    // connect request while leaving, will need to handle
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
      usleep(10000);
      continue;
    }
    break;
  }
  // once all peers have accepted, the lock is considered acquired
  std::lock_guard<std::mutex> metalock_guard(lock_.mutex);
  CHECK(lock_.state == DistributedRWLock::State::ATTEMPTING);
  lock_.state = DistributedRWLock::State::WRITE_LOCKED;
  lock_.holder = PeerId::self();
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
        lock_.cv.notify_one();
        return;
      }
      break;
    case DistributedRWLock::State::ATTEMPTING:
      LOG(FATAL) << "Can't abort lock request";
      break;
    case DistributedRWLock::State::WRITE_LOCKED:
      CHECK(lock_.holder == PeerId::self());
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
      if (peers_.size() == 0) {
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
      lock_.cv.notify_one();
      return;
  }
  metalock.unlock();
}

void Chunk::fillMetadata(proto::ChunkRequestMetadata* destination) {
  CHECK_NOTNULL(destination);
  destination->set_table(underlying_table_->name());
  destination->set_chunk_id(id().hexString());
  destination->set_from_peer(FLAGS_ip_port);
}

bool Chunk::isWriter(const PeerId& peer) {
  std::lock_guard<std::mutex> metalock(lock_.mutex);
  return (lock_.state == DistributedRWLock::State::WRITE_LOCKED &&
      lock_.holder == peer);
}

void Chunk::prepareInitRequest(Message* request) {
  CHECK_NOTNULL(request);
  proto::InitRequest init_request;

  init_request.set_table(underlying_table_->name());
  init_request.set_chunk_id(id().hexString());
  init_request.set_from_peer(FLAGS_ip_port);

  for (const PeerId& swarm_peer : peers_.peers()) {
    init_request.add_peer_address(swarm_peer.ipPort());
  }
  init_request.add_peer_address(PeerId::self().ipPort());

  std::unordered_map<Id, std::shared_ptr<Revision> > data;
  underlying_table_->dump(Time::now(), &data);
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

  distributedWriteLock();
  if (peers_.peers().find(peer) != peers_.peers().end()) {
    distributedUnlock();
    LOG(FATAL) << "Peer requesting to join already in swarm!";
  }
  CHECK(addPeer(peer));
  distributedUnlock();

  response->ack();
}

void Chunk::handleInsertRequest(const Revision& item, Message* response) {
  CHECK_NOTNULL(response);
  CHECK(!isWriter()); // an insert request may not happen while another peer
  // holds the write lock (i.e. inserts must be read-locked). Note that this is
  // not equivalent to checking state != WRITE_LOCKED, as the state may be
  // WRITE_LOCKED at some peers while in reality the lock is not write locked:
  // A lock is only really WRITE_LOCKED when all peers agree that it is.
  // no further locking needed, elegantly
  underlying_table_->patch(item);
  response->ack();
}

void Chunk::handleLeaveRequest(const PeerId& leaver, Message* response) {
  CHECK_NOTNULL(response);
  std::lock_guard<std::mutex> metalock(lock_.mutex);
  CHECK(lock_.state == DistributedRWLock::State::WRITE_LOCKED);
  CHECK_EQ(lock_.holder, leaver);
  peers_.remove(leaver);
  response->impose<Message::kAck>();
}

void Chunk::handleLockRequest(const PeerId& locker, Message* response) {
  CHECK_NOTNULL(response);
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
}

void Chunk::handleNewPeerRequest(const PeerId& peer, const PeerId& sender,
                                 Message* response) {
  CHECK_NOTNULL(response);
  std::lock_guard<std::mutex> metalock(lock_.mutex);
  CHECK(lock_.state == DistributedRWLock::State::WRITE_LOCKED);
  CHECK_EQ(lock_.holder, sender);
  peers_.add(peer);
  response->impose<Message::kAck>();
}

void Chunk::handleUnlockRequest(const PeerId& locker, Message* response) {
  CHECK_NOTNULL(response);
  std::unique_lock<std::mutex> metalock(lock_.mutex);
  CHECK(lock_.state == DistributedRWLock::State::WRITE_LOCKED);
  CHECK(lock_.holder == locker);
  lock_.state = DistributedRWLock::State::UNLOCKED;
  metalock.unlock();
  lock_.cv.notify_one();
  response->impose<Message::kAck>();
}

void Chunk::handleUpdateRequest(const Revision& item, const PeerId& sender,
                                Message* response) {
  CHECK_NOTNULL(response);
  CHECK(isWriter(sender));
  CHECK(dynamic_cast<CRUTable*>(underlying_table_));
  underlying_table_->patch(item);
  response->ack();
}

} // namespace map_api
