#include "map-api/chunk.h"

#include "core.pb.h"

namespace map_api {

bool Chunk::insert(const Revision& item) {
  std::string serialized = item.SerializeAsString();
  proto::HubMessage request;
  request.set_name("net_insert_request"); // TODO(tcies) central declarations
  request.set_serialized(serialized);
  for (const std::weak_ptr<Peer> weak_peer : peers_) {
    std::shared_ptr<Peer> locked_peer = weak_peer.lock();
    CHECK(locked_peer);
    proto::HubMessage response;
    CHECK(locked_peer->request(request, &response));
    CHECK_EQ("ack", response.serialized());
  }
  return false;
}

void Chunk::handleInsertRequest(
    const std::string& serialized_request, proto::HubMessage* response) {
  Revision received;
  CHECK(received.ParseFromString(serialized_request));
  // TODO(tcies) put revision into managed database
  response->set_name("");
  response->set_serialized("ack");
}

} // namespace map_api
