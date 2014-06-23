#include "map-api/chunk.h"

#include "map-api/net-table-manager.h"
#include "map-api/map-api-hub.h"
#include "core.pb.h"
#include "chunk.pb.h"

DECLARE_string(ip_port);

namespace map_api {

const char Chunk::kLeaveRequest[] = "map_api_chunk_leave_request";
const char Chunk::kLockRequest[] = "map_api_chunk_lock_request";
const char Chunk::kNewPeerRequest[] = "map_api_chunk_new_peer_request";
const char Chunk::kUnlockRequest[] = "map_api_chunk_unlock_request";
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(
    Chunk::kLeaveRequest, proto::ChunkRequestMetadata);
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(
    Chunk::kLockRequest, proto::ChunkRequestMetadata);
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(
    Chunk::kNewPeerRequest, proto::NewPeerRequest);
// Same proto as lock request intended - information content is the same
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(
    Chunk::kUnlockRequest, proto::ChunkRequestMetadata);

bool Chunk::init(const Id& id, CRTableRAMCache* underlying_table) {
  CHECK_NOTNULL(underlying_table);
  id_ = id;
  underlying_table_ = underlying_table;
  return true;
}

bool Chunk::init(
    const Id& id, const proto::ConnectResponse& connect_response,
    const PeerId& adder, CRTableRAMCache* underlying_table) {
  CHECK(init(id, underlying_table));
  // connect to peers from connect_response TODO(tcies) notify of self
  CHECK_GT(connect_response.peer_address_size(), 0);
  for (int i = 0; i < connect_response.peer_address_size(); ++i) {
    peers_.add(PeerId(connect_response.peer_address(i)));
  }
  // feed data from connect_response into underlying table TODO(tcies) piecewise
  for (int i = 0; i < connect_response.serialized_revision_size(); ++i) {
    Revision data;
    CHECK(data.ParseFromString((connect_response.serialized_revision(i))));
    CHECK(underlying_table->insert(&data));
    //TODO(tcies) problematic with CRU tables, table::serialize()
  }
  std::lock_guard<std::mutex> metalock(lock_.mutex);
  lock_.state = DistributedRWLock::State::WRITE_LOCKED;
  lock_.holder = adder;
  return true;
}

Id Chunk::id() const {
  // TODO(tcies) implement
  return id_;
}

bool Chunk::insert(const Revision& item) {
  proto::InsertRequest insert_request;
  insert_request.set_chunk_id(id().hexString());
  insert_request.set_serialized_revision(item.SerializeAsString());
  Message request;
  request.impose<NetTableManager::kInsertRequest, proto::InsertRequest>(
      insert_request);
  CHECK(peers_.undisputableBroadcast(request));
  return true;
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
  CHECK(peers_.undisputableBroadcast(request));
  relinquished_ = true;
  distributedUnlock(); // i.e. must be able to handle unlocks from outside
  // the swarm. Slightly unclean design maybe but IMO not too disturbing
}

int Chunk::requestParticipation() const {
  proto::ParticipationRequest participation_request;
  participation_request.set_table(underlying_table_->name());
  participation_request.set_chunk_id(id().hexString());
  participation_request.set_from_peer(FLAGS_ip_port);
  Message request;
  request.impose<NetTableManager::kParticipationRequest,
  proto::ParticipationRequest>(participation_request);
  std::unordered_map<PeerId, Message> responses;
  MapApiHub::instance().broadcast(request, &responses);
  // TODO(tcies) only request those who are not present yet
  // at this point, the handler thread should have processed all resulting
  // chunk connection requests
  int new_participant_count = 0;
  for (const std::pair<PeerId, Message>& response : responses) {
    if (response.second.isType<Message::kAck>()){
      ++new_participant_count;
    }
  }
  return new_participant_count;
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
      // renew the reading lease
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
      Message request, response;
      proto::ChunkRequestMetadata unlock_request;
      fillMetadata(&unlock_request);
      request.impose<kUnlockRequest>(unlock_request);
      // to make sure that possibly concurrent locking works correctly, we
      // need to unlock in reverse order of locking, i.e. we must ensure that
      // if peer with address A considers the lock unlocked, any peer B > A
      // (including the local one) does as well
      bool self_unlocked = false;
      if (*peers_.peers().end() < PeerId::self()) {
        lock_.state = DistributedRWLock::State::UNLOCKED;
        self_unlocked = true;
      }
      for (std::set<PeerId>::const_reverse_iterator rit =
          peers_.peers().rbegin(); rit != peers_.peers().rend(); ++rit) {
        if (!self_unlocked && *rit < PeerId::self()) {
          lock_.state = DistributedRWLock::State::UNLOCKED;
          self_unlocked = true;
        }
        MapApiHub::instance().request(*rit, request, &response);
        CHECK(response.isType<Message::kAck>());
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

void Chunk::handleConnectRequest(const PeerId& peer, Message* response) {
  CHECK_NOTNULL(response);
  proto::ConnectResponse connect_response;

  // stop all chunk modification, lock to self
  distributedWriteLock();
  if (peers_.peers().find(peer) != peers_.peers().end()) {
    distributedUnlock();
    LOG(FATAL) << "Peer requesting to join already in swarm!";
  }
  // message to newly connected peer
  for (const PeerId& swarm_peer : peers_.peers()) {
    connect_response.add_peer_address(swarm_peer.ipPort());
  }
  std::unordered_map<Id, std::shared_ptr<Revision> > data;
  underlying_table_->dump(Time::now(), &data);
  for (const std::pair<const Id, std::shared_ptr<Revision> >& data_pair :
      data) {
    connect_response.add_serialized_revision(
        data_pair.second->SerializeAsString());
  }
  response->impose<NetTableManager::kConnectResponse, proto::ConnectResponse>(
      connect_response);
  // message to current swarm
  Message request;
  proto::NewPeerRequest new_peer_request;
  new_peer_request.set_table(underlying_table_->name());
  new_peer_request.set_chunk_id(id().hexString());
  new_peer_request.set_new_peer(peer.ipPort());
  new_peer_request.set_from_peer(FLAGS_ip_port);
  request.impose<kNewPeerRequest>(new_peer_request);
  CHECK(peers_.undisputableBroadcast(request));
  // add peer
  peers_.add(peer);
  distributedUnlock();
}

void Chunk::handleInsertRequest(const Revision& item, Message* response) {
  CHECK_NOTNULL(response);
  // TODO(tcies) implement
  CHECK(false);
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

} // namespace map_api
