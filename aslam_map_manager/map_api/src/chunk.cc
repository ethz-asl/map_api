#include "map-api/chunk.h"

#include "core.pb.h"

#include "map-api/chunk-manager.h"
#include "chunk.pb.h"

namespace map_api {

bool Chunk::init(const Id& id, const proto::ConnectResponse& connect_response,
                 CRTable* underlying_table) {
  id_ = id;
  CHECK_NOTNULL(underlying_table);
  underlying_table_ = underlying_table;
  // connect to peers from connect_response TODO(tcies) notify of self
  CHECK_LT(0, connect_response.peer_address_size());
  for (int i = 0; i < connect_response.peer_address_size(); ++i) {
    peers_.ensure(connect_response.peer_address(i));
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
