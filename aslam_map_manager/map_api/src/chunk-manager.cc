#include "map-api/chunk-manager.h"

#include <gflags/gflags.h>

#include "map-api/cr-table.h"
#include "map-api/map-api-hub.h"
#include "map-api/net-table-manager.h"
#include "chunk.pb.h"

DECLARE_string(ip_port);

namespace map_api {

bool ChunkManager::init(CRTableRAMCache* underlying_table) {
  CHECK_NOTNULL(underlying_table);
  cache_ = underlying_table;
  return true;
}

std::weak_ptr<Chunk> ChunkManager::connectTo(const Id& chunk_id,
                                             const PeerId& peer) {
  Message request, response;
  // sends request of chunk info to peer
  proto::ConnectRequest connect_request;
  connect_request.set_chunk_id(chunk_id.hexString());
  connect_request.set_from_peer(FLAGS_ip_port);
  request.impose<NetTableManager::kConnectRequest, proto::ConnectRequest>(
      connect_request);
  // TODO(tcies) add to local peer subset instead
  MapApiHub::instance().request(peer, request, &response);
  CHECK(response.isType<NetTableManager::kConnectResponse>());
  proto::ConnectResponse connect_response;
  CHECK(connect_response.ParseFromString(response.serialized()));
  // receives peer list and data from peer, forwards it to chunk init()
  std::shared_ptr<Chunk> chunk;
  chunk.reset(new Chunk);
  CHECK(chunk->init(chunk_id, connect_response, cache_));
  active_chunks_[chunk_id] = chunk;
  return std::weak_ptr<Chunk>(chunk);
}

std::weak_ptr<Chunk> ChunkManager::newChunk() {
  Id chunk_id = Id::random();
  std::shared_ptr<Chunk> chunk = std::shared_ptr<Chunk>(new Chunk);
  CHECK(chunk->init(chunk_id, cache_));
  active_chunks_[chunk_id] = chunk;
  return std::weak_ptr<Chunk>(chunk);
}

int ChunkManager::findAmongPeers(
    const CRTable& table, const std::string& key, const Revision& valueHolder,
    const Time& time,
    std::unordered_map<Id, std::shared_ptr<Revision> >* dest) {
  // TODO(tcies) implement
  return 0;
}

} // namespace map_api
