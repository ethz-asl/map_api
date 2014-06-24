#include "map-api/net-table.h"

#include <glog/logging.h>

#include "map-api/cr-table-ram-cache.h"
#include "map-api/cru-table-ram-cache.h"
#include "map-api/map-api-core.h"
#include "map-api/net-table-manager.h"

DECLARE_string(ip_port);

namespace map_api {

const std::string NetTable::kChunkIdField = "chunk_id";

bool NetTable::init(
    bool updateable, std::unique_ptr<TableDescriptor>* descriptor) {
  updateable_ = updateable;
  (*descriptor)->addField<Id>(kChunkIdField);
  if (updateable) {
    cache_.reset(new CRUTableRAMCache);
  }
  else {
    cache_.reset(new CRTableRAMCache);
  }
  CHECK(cache_->init(descriptor));
  return true;
}

std::shared_ptr<Revision> NetTable::getTemplate() const {
  return cache_->getTemplate();
}

std::weak_ptr<Chunk> NetTable::newChunk() {
  Id chunk_id = Id::random();
  std::shared_ptr<Chunk> chunk = std::shared_ptr<Chunk>(new Chunk);
  CHECK(chunk->init(chunk_id, cache_.get()));
  active_chunks_[chunk_id] = chunk;
  return std::weak_ptr<Chunk>(chunk);
}

std::weak_ptr<Chunk> NetTable::getChunk(const Id& chunk_id) {
  ChunkMap::iterator found = active_chunks_.find(chunk_id);
  CHECK(found != active_chunks_.end());
  return std::weak_ptr<Chunk>(found->second);
}

bool NetTable::insert(const std::weak_ptr<Chunk>& chunk, Revision* query) {
  CHECK_NOTNULL(query);
  std::shared_ptr<Chunk> locked_chunk = chunk.lock();
  CHECK(locked_chunk);
  CHECK(locked_chunk->insert(query)); // TODO(tcies) insert into cache in here
  return true;
}

bool NetTable::update(Revision* query) {
  CHECK_NOTNULL(query);
  CHECK(updateable_);
  Id chunk_id;
  query->get(kChunkIdField, &chunk_id);
  std::weak_ptr<Chunk> chunk = getChunk(chunk_id);
  // TODO(tcies) revisit weak pointer business
  std::shared_ptr<Chunk> locked_chunk = chunk.lock();
  CHECK(locked_chunk);
  CHECK(locked_chunk->update(query));
  return true;
}

void NetTable::dumpCache(
    const Time& time,
    std::unordered_map<Id, std::shared_ptr<Revision> >* destination) {
  CHECK_NOTNULL(destination);
  // TODO(tcies) lock cache access
  cache_->dump(time, destination);
}

bool NetTable::has(const Id& chunk_id) const {
  return active_chunks_.find(chunk_id) != active_chunks_.end();
}

std::weak_ptr<Chunk> NetTable::connectTo(const Id& chunk_id,
                                           const PeerId& peer) {
  Message request, response;
  // sends request of chunk info to peer
  proto::ConnectRequest connect_request;
  connect_request.set_table(cache_->name());
  connect_request.set_chunk_id(chunk_id.hexString());
  connect_request.set_from_peer(FLAGS_ip_port);
  request.impose<Chunk::kConnectRequest, proto::ConnectRequest>(
      connect_request);
  // TODO(tcies) add to local peer subset instead - peers in ChunkManager?
  // meld ChunkManager and NetCRTable?
  MapApiHub::instance().request(peer, request, &response);
  CHECK(response.isType<Message::kAck>());
  ChunkMap::iterator found = active_chunks_.find(chunk_id);
  CHECK(found != active_chunks_.end());
  return std::weak_ptr<Chunk>(found->second);
}

void NetTable::leaveAllChunks() {
  for (const std::pair<const Id, std::shared_ptr<Chunk> >& chunk :
      active_chunks_) {
    chunk.second->leave();
  }
  active_chunks_.clear();
}

void NetTable::handleConnectRequest(const Id& chunk_id, const PeerId& peer,
                                      Message* response) {
  ChunkMap::iterator found;
  if (routingBasics(chunk_id, response, &found)) {
    found->second->handleConnectRequest(peer, response);
  }
}

void NetTable::handleInitRequest(
    const proto::InitRequest& request, Message* response) {
  CHECK_NOTNULL(response);
  LOG(INFO) << "Received init request for table " << request.table() <<
      " chunk " << request.chunk_id();
  Id chunk_id;
  CHECK(chunk_id.fromHexString(request.chunk_id()));
  if (MapApiCore::instance().tableManager().getTable(request.table()).
      has(chunk_id)) {
    response->impose<Message::kRedundant>();
    return;
  }
  std::shared_ptr<Chunk> chunk = std::shared_ptr<Chunk>(new Chunk);
  CHECK(chunk->init(chunk_id, request, cache_.get()));
  active_chunks_[chunk_id] = chunk;
  LOG(INFO) << PeerId::self() << " now has " << active_chunks_.size() <<
      " chunks in table " << cache_->name();
  response->impose<Message::kAck>();
}

void NetTable::handleInsertRequest(
    const Id& chunk_id, const Revision& item, Message* response) {
  ChunkMap::iterator found;
  if (routingBasics(chunk_id, response, &found)) {
    found->second->handleInsertRequest(item, response);
  }
}

void NetTable::handleLeaveRequest(
    const Id& chunk_id, const PeerId& leaver, Message* response) {
  ChunkMap::iterator found;
  if (routingBasics(chunk_id, response, &found)) {
    found->second->handleLeaveRequest(leaver, response);
  }
}

void NetTable::handleLockRequest(
    const Id& chunk_id, const PeerId& locker, Message* response) {
  ChunkMap::iterator found;
  if (routingBasics(chunk_id, response, &found)) {
    found->second->handleLockRequest(locker, response);
  }
}

void NetTable::handleNewPeerRequest(
    const Id& chunk_id, const PeerId& peer, const PeerId& sender,
    Message* response) {
  ChunkMap::iterator found;
  if (routingBasics(chunk_id, response, &found)) {
    found->second->handleNewPeerRequest(peer, sender, response);
  }
}

void NetTable::handleUnlockRequest(
    const Id& chunk_id, const PeerId& locker, Message* response) {
  ChunkMap::iterator found;
  if (routingBasics(chunk_id, response, &found)) {
    found->second->handleUnlockRequest(locker, response);
  }
}

bool NetTable::routingBasics(
    const Id& chunk_id, Message* response, ChunkMap::iterator* found) {
  CHECK_NOTNULL(response);
  CHECK_NOTNULL(found);
  *found = active_chunks_.find(chunk_id);
  if (*found == active_chunks_.end()) {
    response->impose<Message::kDecline>();
    return false;
  }
  return true;
}

} // namespace map_api
