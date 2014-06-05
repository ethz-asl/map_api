#include "map-api/chunk.h"

#include "core.pb.h"

namespace map_api {

const char Chunk::kInsertRequest[] = "map_api_chunk_insert";
template<>
void Message::impose<Chunk::kInsertRequest,Revision>(const Revision& item) {
  this->set_type(Chunk::kInsertRequest);
  this->set_serialized(item.SerializeAsString());
}

bool Chunk::insert(const Revision& item) {
  Message request;
  request.impose<kInsertRequest, Revision>(item);
  for (const std::weak_ptr<Peer> weak_peer : peers_) {
    std::shared_ptr<Peer> locked_peer = weak_peer.lock();
    CHECK(locked_peer);
    Message response;
    CHECK(locked_peer->request(request, &response));
    // TODO(tcies) means to verify Message type
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
