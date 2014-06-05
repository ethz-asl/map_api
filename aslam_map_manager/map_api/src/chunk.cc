#include "map-api/chunk.h"

#include "core.pb.h"

#include "map-api/chunk-manager.h"
#include "chunk.pb.h"

namespace map_api {

Id Chunk::id() {
  // TODO(tcies) implement
  return Id();
}

bool Chunk::insert(const Revision& item) {
  proto::InsertRequest insert_request;
  insert_request.set_chunk_id(id().hexString());
  insert_request.set_serialized_revision(item.SerializeAsString());
  Message request;
  request.impose<ChunkManager::kInsertRequest, proto::InsertRequest>(
      insert_request);
  for (const std::weak_ptr<Peer> weak_peer : peers_) {
    std::shared_ptr<Peer> locked_peer = weak_peer.lock();
    CHECK(locked_peer);
    Message response;
    CHECK(locked_peer->request(request, &response));
    CHECK(response.isType<Message::kAck>());
  }
  return true;
}

bool Chunk::handleInsert(const Revision& item) {
  // TODO(tcies) implement
  return false;
}

} // namespace map_api
