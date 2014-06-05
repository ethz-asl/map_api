#include "map-api/chunk.h"

#include "core.pb.h"

namespace map_api {

const char Chunk::kInsertRequest[] = "map_api_chunk_insert";
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(Chunk::kInsertRequest,Revision);

bool Chunk::insert(const Revision& item) {
  Message request;
  request.impose<kInsertRequest, Revision>(item);
  for (const std::weak_ptr<Peer> weak_peer : peers_) {
    std::shared_ptr<Peer> locked_peer = weak_peer.lock();
    CHECK(locked_peer);
    Message response;
    CHECK(locked_peer->request(request, &response));
    CHECK(response.isType<Message::kAck>());
  }
  return false;
}

void Chunk::handleInsertRequest(
    const std::string& serialized_request, Message* response) {
  Revision received;
  CHECK(received.ParseFromString(serialized_request));
  // TODO(tcies) put revision into managed database
  response->impose<Message::kAck>();
}

} // namespace map_api
