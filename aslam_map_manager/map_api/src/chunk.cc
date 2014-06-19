#include "map-api/chunk.h"

#include "map-api/net-table-manager.h"
#include "map-api/map-api-hub.h"
#include "core.pb.h"
#include "chunk.pb.h"

DECLARE_string(ip_port);

namespace map_api {

bool Chunk::init(const Id& id, CRTableRAMCache* underlying_table) {
  id_ = id;
  CHECK_NOTNULL(underlying_table);
  underlying_table_ = underlying_table;
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
  // at this point, the server handler should have processed all ensuing
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
  // TODO(tcies) will need more concurency control: What happens exactly if
  // one peer wants to add/update data while another one is handling a
  // connection request? : Lock chunk
  // TODO(tcies) populate connect_response with chunk revisions
  response->impose<NetTableManager::kConnectResponse, proto::ConnectResponse>(
      connect_response);
  peers_.add(peer);
  // TODO(tcies) notify other peers of this peer joining the swarm
}

void Chunk::distributedReadLock(DistributedRWLock* lock) {
  CHECK_NOTNULL(lock);
  std::unique_lock<std::mutex> metalock(lock->mutex);
  while (lock->state != DistributedRWLock::UNLOCKED &&
      lock->state != DistributedRWLock::READ_LOCKED) {
    lock->cv.wait(metalock);
  }
  lock->state = DistributedRWLock::READ_LOCKED;
  ++lock->n_readers;
  metalock.unlock();
}

void Chunk::distributedWriteLock(DistributedRWLock* lock) {
  CHECK_NOTNULL(lock);
}

void Chunk::handleLockRequest(const PeerId& locker, Message* response) {
  CHECK_NOTNULL(response);
}

const char Chunk::kLockRequest[] = "map_api_chunk_lock_request";

void Chunk::distributedUnlock(DistributedRWLock* lock) {
  CHECK_NOTNULL(lock);
}

void Chunk::handleUnlockRequest(const PeerId& locker, Message* response) {
  CHECK_NOTNULL(response);
}

const char Chunk::kUnlockRequest[] = "map_api_chunk_unlock_request";

} // namespace map_api
