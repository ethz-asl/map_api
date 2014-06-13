#include "map-api/chunk.h"

#include "core.pb.h"

#include "map-api/chunk-manager.h"
#include "map-api/map-api-hub.h"
#include "chunk.pb.h"

DECLARE_string(ip_port);

namespace map_api {

bool Chunk::init(const Id& id, const proto::ConnectResponse& connect_response,
                 CRTable* underlying_table) {
  id_ = id;
  CHECK_NOTNULL(underlying_table);
  underlying_table_ = underlying_table;
  // connect to peers from connect_response TODO(tcies) notify of self
  CHECK_LT(0, connect_response.peer_address_size());
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
  request.impose<ChunkManager::kInsertRequest, proto::InsertRequest>(
      insert_request);
  CHECK(peers_.undisputable_broadcast(request));
  return true;
}

bool Chunk::handleInsert(const Revision& item) {
  // TODO(tcies) implement
  return false;
}

int Chunk::requestParticipation() const {
  proto::ParticipationRequest participation_request;
  participation_request.set_chunk_id(id().hexString()); // TODO(tcies) table
  participation_request.set_from_peer(FLAGS_ip_port);
  Message request;
  request.impose<ChunkManager::kParticipationRequest,
  proto::ParticipationRequest>(participation_request);
  // TODO(tcies) strongly type peer address or, better, use peer weak pointer
  std::unordered_map<PeerId, Message> responses;
  MapApiHub::instance().broadcast(request, &responses);
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

} // namespace map_api
