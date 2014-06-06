#include "map-api/chunk-manager.h"

#include "map-api/cr-table.h"
#include "map-api/map-api-hub.h"
#include "chunk.pb.h"

namespace map_api {

MEYERS_SINGLETON_INSTANCE_FUNCTION_IMPLEMENTATION(ChunkManager);

ChunkManager::~ChunkManager() {}

bool ChunkManager::init() {
  MapApiHub::instance().registerHandler(kInsertRequest, handleInsertRequest);
  return true;
}

std::weak_ptr<Chunk> ChunkManager::newChunk(const CRTable& table) {
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

// ==============
// INSERT REQUEST
// ==============

void ChunkManager::handleInsertRequest(
    const std::string& serialized_request, Message* response) {
  CHECK_NOTNULL(response);
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
  response->impose<Message::kAck>();
}

const char ChunkManager::kInsertRequest[] = "map_api_chunk_insert";
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(ChunkManager::kInsertRequest,
                                     proto::InsertRequest);
const char ChunkManager::kChunkNotOwned[] = "map_api_chunk_not_owned";

} // namespace map_api
