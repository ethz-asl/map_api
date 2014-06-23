#include "map-api/net-cr-table.h"

#include <glog/logging.h>

#include "map-api/map-api-hub.h"
#include "map-api/net-table-manager.h"

DECLARE_string(ip_port);

namespace map_api {

const std::string NetCRTable::kChunkIdField = "chunk_id";

bool NetCRTable::init(std::unique_ptr<TableDescriptor>* descriptor) {
  (*descriptor)->addField<Id>(kChunkIdField);
  cache_.reset(new CRTableRAMCache);
  CHECK(cache_->init(descriptor));
  return true;
}

std::shared_ptr<Revision> NetCRTable::getTemplate() const {
  return cache_->getTemplate();
}

std::weak_ptr<Chunk> NetCRTable::newChunk() {
  Id chunk_id = Id::random();
  std::shared_ptr<Chunk> chunk = std::shared_ptr<Chunk>(new Chunk);
  CHECK(chunk->init(chunk_id, cache_.get()));
  active_chunks_[chunk_id] = chunk;
  return std::weak_ptr<Chunk>(chunk);
}

bool NetCRTable::insert(const std::weak_ptr<Chunk>& chunk, Revision* query) {
  CHECK_NOTNULL(query);
  std::shared_ptr<Chunk> locked_chunk = chunk.lock();
  CHECK(locked_chunk);
  query->set(kChunkIdField, locked_chunk->id());
  CHECK(cache_->insert(query));
  CHECK(locked_chunk->insert(*query)); // TODO(tcies) insert into cache in here
  return true;
}

void NetCRTable::dumpCache(
    const Time& time,
    std::unordered_map<Id, std::shared_ptr<Revision> >* destination) {
  CHECK_NOTNULL(destination);
  // TODO(tcies) lock cache access
  cache_->dump(time, destination);
}

bool NetCRTable::has(const Id& chunk_id) const {
  return active_chunks_.find(chunk_id) != active_chunks_.end();
}

std::weak_ptr<Chunk> NetCRTable::connectTo(const Id& chunk_id,
                                           const PeerId& peer) {
  Message request, response;
  // sends request of chunk info to peer
  proto::ConnectRequest connect_request;
  connect_request.set_table(cache_->name());
  connect_request.set_chunk_id(chunk_id.hexString());
  connect_request.set_from_peer(FLAGS_ip_port);
  request.impose<NetTableManager::kConnectRequest, proto::ConnectRequest>(
      connect_request);
  // TODO(tcies) add to local peer subset instead - peers in ChunkManager?
  // meld ChunkManager and NetCRTable?
  MapApiHub::instance().request(peer, request, &response);
  CHECK(response.isType<NetTableManager::kConnectResponse>());
  proto::ConnectResponse connect_response;
  CHECK(connect_response.ParseFromString(response.serialized()));
  // receives peer list and data from peer, forwards it to chunk init().
  // Also need to add the peer we have been communicating with to the swarm
  // list, as it doesn't have itself in its swarm list
  connect_response.add_peer_address(peer.ipPort());
  std::shared_ptr<Chunk> chunk(new Chunk);
  CHECK(chunk->init(chunk_id, connect_response, peer, cache_.get()));
  active_chunks_[chunk_id] = chunk;
  return std::weak_ptr<Chunk>(chunk);
}

void NetCRTable::handleConnectRequest(const Id& chunk_id, const PeerId& peer,
                                      Message* response) {
  CHECK_NOTNULL(response);
  // TODO(tcies) lock chunk against removal, monitor style access to chunks
  ChunkMap::iterator found = active_chunks_.find(chunk_id);
  if (found == active_chunks_.end()) {
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleConnectRequest(peer, response);
}

} // namespace map_api
