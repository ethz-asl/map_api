#include "map-api/chunk.h"

#include "core.pb.h"

#include "map-api/chunk-manager.h"
#include "chunk.pb.h"

namespace map_api {

Id Chunk::id() const {
  // TODO(tcies) implement
  return id_;
}

bool Chunk::insert(const Revision& item) {
  proto::InsertRequest insert_request;
  insert_request.set_chunk_id(id().hexString());
  insert_request.set_serialized_revision(item.SerializeAsString());
  Message request;
  request.impose<ChunkManager::kInsertRequest, proto::InsertRequest>(
      insert_request);
  CHECK(peers_.undisputable_broadcast(request));
  return true;
}

bool Chunk::handleInsert(const Revision& item) {
  // TODO(tcies) implement
  return false;
}

} // namespace map_api
