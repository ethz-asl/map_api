#include "map-api/chunk-manager.h"

#include <gflags/gflags.h>

#include "map-api/cr-table.h"
#include "map-api/map-api-hub.h"
#include "chunk.pb.h"

DECLARE_string(ip_port);

namespace map_api {

bool ChunkManager::init(CRTableRAMCache* underlying_table) {
  CHECK_NOTNULL(underlying_table);
  cache_ = underlying_table;
  // TODO(tcies) static-init for the following two?
  MapApiHub::instance().registerHandler(kInsertRequest, handleInsertRequest);
  MapApiHub::instance().registerHandler(kParticipationRequest,
                                        handleParticipationRequest);
  return true;
}

const char ChunkManager::kConnectRequest[] = "map_api_chunk_connect";
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(ChunkManager::kConnectRequest,
                                     proto::ConnectRequest);
const char ChunkManager::kConnectResponse[] = "map_api_chunk_connect_response";
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(ChunkManager::kConnectResponse,
                                     proto::ConnectResponse);
std::weak_ptr<Chunk> ChunkManager::connectTo(const Id& chunk_id,
                                             const PeerId& peer) {
  Message request, response;
  // sends request of chunk info to peer
  proto::ConnectRequest connect_request;
  connect_request.set_chunk_id(chunk_id.hexString());
  connect_request.set_from_peer(FLAGS_ip_port);
  request.impose<kConnectRequest, proto::ConnectRequest>(connect_request);
  // TODO(tcies) add to local peer subset instead
  MapApiHub::instance().request(peer, request, &response);
  CHECK(response.isType<kConnectResponse>());
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

const char ChunkManager::kParticipationRequest[] =
    "map_api_chunk_participation";
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(ChunkManager::kParticipationRequest,
                                     proto::ParticipationRequest);

// ==================
// CONNECTION REQUEST
// ==================
void ChunkManager::handleConnectRequest(const std::string& serialized_request,
                                        Message* response) {
  // TODO(tcies) implement
}

// ==============
// INSERT REQUEST
// ==============
void ChunkManager::handleInsertRequest(
    const std::string& serialized_request, Message* response) {
  CHECK_NOTNULL(response);
  /** TODO(tcies) re-enable with TableManager
  // parse message TODO(tcies) centralize process?
  proto::InsertRequest insert_request;
  CHECK(insert_request.ParseFromString(serialized_request));
  // determine addressed chunk
  Id requested_chunk;
  CHECK(requested_chunk.fromHexString(insert_request.chunk_id()));
  ChunkMap::iterator chunk_iterator =
      instance().active_chunks_.find(requested_chunk);
  if (chunk_iterator == instance().active_chunks_.end()) {
    response->impose<kChunkNotOwned>();
    return;
  }
  std::shared_ptr<Chunk> addressedChunk = chunk_iterator->second;
  // insert revision into chunk
  Revision to_insert;
  to_insert.ParseFromString(insert_request.serialized_revision());
  CHECK(addressedChunk->handleInsert(to_insert));
  */
  response->impose<Message::kAck>();
}
const char ChunkManager::kInsertRequest[] = "map_api_chunk_insert";
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(ChunkManager::kInsertRequest,
                                     proto::InsertRequest);
const char ChunkManager::kChunkNotOwned[] = "map_api_chunk_not_owned";

// =====================
// PARTICIPATION REQUEST
// =====================
void ChunkManager::handleParticipationRequest(
    const std::string& serialized_request, Message* response) {
  CHECK_NOTNULL(response);
  // TODO(tcies) implement
  response->impose<Message::kAck>();
}
} // namespace map_api
