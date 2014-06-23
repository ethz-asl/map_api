#include "map-api/chunk.h"

#include "map-api/net-table-manager.h"
#include "map-api/map-api-hub.h"
#include "core.pb.h"
#include "chunk.pb.h"

DECLARE_string(ip_port);

namespace map_api {

const std::string Chunk::kJoinLock = "join";
const std::string Chunk::kUpdateLock = "update";

bool Chunk::init(const Id& id, CRTableRAMCache* underlying_table) {
  CHECK_NOTNULL(underlying_table);
  id_ = id;
  underlying_table_ = underlying_table;
  locks_[kJoinLock].reset(new DistributedRWLock);
  locks_[kUpdateLock].reset(new DistributedRWLock);
  return true;
}

bool Chunk::init(const Id& id, const proto::ConnectResponse& connect_response,
                 CRTableRAMCache* underlying_table) {
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
    //TODO(tcies) problematic with CRU tables
  }
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

bool Chunk::handleInsert(const Revision& item) {
  // TODO(tcies) implement
  return false;
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

void Chunk::handleConnectRequest(const PeerId& peer, Message* response) {
  CHECK_NOTNULL(response);
  // TODO(tcies) what if peer already connected?
  proto::ConnectResponse connect_response;
  for (const PeerId& peer : peers_.peers()) {
    connect_response.add_peer_address(peer.ipPort());
  }
  // TODO(tcies) will need more concurrency control: What happens exactly if
  // one peer wants to add/update data while another one is handling a
  // connection request? : Lock chunk
  // TODO(tcies) populate connect_response with chunk revisions
  response->impose<NetTableManager::kConnectResponse, proto::ConnectResponse>(
      connect_response);
  peers_.add(peer);
  // TODO(tcies) notify other peers of this peer joining the swarm
}

const char Chunk::kLockRequest[] = "map_api_chunk_lock_request";
const char Chunk::kUnlockRequest[] = "map_api_chunk_unlock_request";
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(Chunk::kLockRequest, proto::LockRequest);
// Same proto intended - information content is the same
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(Chunk::kUnlockRequest, proto::LockRequest);

void Chunk::distributedReadLock(const std::string& lock_name) {
  CHECK(false);
  DistributedRWLock& lock = getLock(lock_name);
  std::unique_lock<std::mutex> metalock(lock.mutex);
  while (lock.state != DistributedRWLock::State::UNLOCKED &&
      lock.state != DistributedRWLock::State::READ_LOCKED) {
    lock.cv.wait(metalock);
  }
  lock.state = DistributedRWLock::State::READ_LOCKED;
  ++lock.n_readers;
  metalock.unlock();
}

void Chunk::distributedWriteLock(const std::string& lock_name) {
  DistributedRWLock& lock = getLock(lock_name);
  while(true) { // lock: attempt until success
    std::unique_lock<std::mutex> metalock(lock.mutex);
    while (lock.state != DistributedRWLock::State::UNLOCKED &&
        lock.state != DistributedRWLock::State::ATTEMPTING) {
      lock.cv.wait(metalock);
    }
    lock.state = DistributedRWLock::State::ATTEMPTING;
    // unlocking metalock to avoid deadlocks when two peers try to acquire the
    // lock
    metalock.unlock();

    Message request, response;
    proto::LockRequest lock_request;
    lock_request.set_table(underlying_table_->name());
    lock_request.set_chunk_id(id().hexString());
    lock_request.set_from_peer(FLAGS_ip_port);
    lock_request.set_type(lock_name);
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
  std::lock_guard<std::mutex> metalock_guard(lock.mutex);
  CHECK(lock.state == DistributedRWLock::State::ATTEMPTING);
  lock.state = DistributedRWLock::State::WRITE_LOCKED;
  lock.holder = PeerId::self();
}

void Chunk::handleLockRequest(const PeerId& locker,
                              const std::string& lock_name, Message* response) {
  CHECK_NOTNULL(response);
  DistributedRWLock& lock = getLock(lock_name);
  std::unique_lock<std::mutex> metalock(lock.mutex);
  // TODO(tcies) as mentioned before - respond immediately and pulse instead
  while (lock.state == DistributedRWLock::State::READ_LOCKED) {
    lock.cv.wait(metalock);
  }
  switch (lock.state) {
    case DistributedRWLock::State::UNLOCKED:
      lock.state = DistributedRWLock::State::WRITE_LOCKED;
      lock.holder = locker;
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
        lock.state = DistributedRWLock::State::WRITE_LOCKED;
        lock.holder = locker;
        response->impose<Message::kAck>();
      }
      break;
    case DistributedRWLock::State::WRITE_LOCKED:
      response->impose<Message::kDecline>();
      break;
  }
  metalock.unlock();
}

void Chunk::distributedUnlock(const std::string& lock_name) {
  DistributedRWLock& lock = getLock(lock_name);
  std::unique_lock<std::mutex> metalock(lock.mutex);
  switch (lock.state) {
    case DistributedRWLock::State::UNLOCKED:
      LOG(FATAL) << "Attempted to unlock already unlocked lock";
      break;
    case DistributedRWLock::State::READ_LOCKED:
      if (!--lock.n_readers) {
        lock.state = DistributedRWLock::State::UNLOCKED;
        metalock.unlock();
        lock.cv.notify_one();
        return;
      }
      break;
    case DistributedRWLock::State::ATTEMPTING:
      LOG(FATAL) << "Can't abort lock request";
      break;
    case DistributedRWLock::State::WRITE_LOCKED:
      CHECK(lock.holder == PeerId::self());
      Message request, response;
      proto::LockRequest unlock_request;
      unlock_request.set_table(underlying_table_->name());
      unlock_request.set_chunk_id(id().hexString());
      unlock_request.set_from_peer(FLAGS_ip_port);
      unlock_request.set_type(lock_name);
      request.impose<kUnlockRequest>(unlock_request);
      // to make sure that possibly concurrent locking works correctly, we
      // need to unlock in reverse order of locking, i.e. we must ensure that
      // if peer with address A considers the lock unlocked, any peer B > A
      // (including the local one) does as well
      bool self_unlocked = false;
      if (*peers_.peers().end() < PeerId::self()) {
        lock.state = DistributedRWLock::State::UNLOCKED;
        self_unlocked = true;
      }
      for (std::set<PeerId>::const_reverse_iterator rit =
          peers_.peers().rbegin(); rit != peers_.peers().rend(); ++rit) {
        if (!self_unlocked && *rit < PeerId::self()) {
          lock.state = DistributedRWLock::State::UNLOCKED;
          self_unlocked = true;
        }
        MapApiHub::instance().request(*rit, request, &response);
        CHECK(response.isType<Message::kAck>());
      }
      metalock.unlock();
      lock.cv.notify_one();
      return;
  }
  metalock.unlock();
}

void Chunk::handleUnlockRequest(
    const PeerId& locker, const std::string& lock_name, Message* response) {
  CHECK_NOTNULL(response);
  DistributedRWLock& lock = getLock(lock_name);
  std::unique_lock<std::mutex> metalock(lock.mutex);
  CHECK(lock.state == DistributedRWLock::State::WRITE_LOCKED);
  CHECK(lock.holder == locker);
  lock.state = DistributedRWLock::State::UNLOCKED;
  metalock.unlock();
  lock.cv.notify_one();
  response->impose<Message::kAck>();
}

Chunk::DistributedRWLock& Chunk::getLock(const std::string& lock_name) {
  std::unordered_map<std::string,
  std::unique_ptr<DistributedRWLock> >::iterator found =
      locks_.find(lock_name);
  CHECK(found != locks_.end());
  CHECK_NOTNULL(found->second.get());
  return *found->second;
}
const Chunk::DistributedRWLock& Chunk::getLock(const std::string& lock_name)
const {
  std::unordered_map<std::string,
  std::unique_ptr<DistributedRWLock> >::const_iterator found =
      locks_.find(lock_name);
  CHECK(found != locks_.end());
  CHECK_NOTNULL(found->second.get());
  return *found->second;
}

} // namespace map_api
