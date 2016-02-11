#include <dmap/server-discovery.h>
#include <fstream>  // NOLINT
#include <sstream>  // NOLINT
#include <string>

#include <sys/file.h>  // linux-specific

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "dmap/hub.h"
#include "./core.pb.h"

namespace dmap {

const char ServerDiscovery::kAnnounceRequest[] =
    "dmap_server_discovery_announce_request";
const char ServerDiscovery::kGetPeersRequest[] =
    "dmap_server_discovery_get_peers_request";
const char ServerDiscovery::kGetPeersResponse[] =
    "dmap_server_discovery_get_peers_response";
MAP_API_PROTO_MESSAGE(ServerDiscovery::kGetPeersResponse,
                      proto::ServerDiscoveryGetPeersResponse);
const char ServerDiscovery::kLockRequest[] =
    "dmap_server_discovery_lock_request";
const char ServerDiscovery::kRemoveRequest[] =
    "dmap_server_discovery_remove_request";
MAP_API_STRING_MESSAGE(ServerDiscovery::kRemoveRequest);
const char ServerDiscovery::kUnlockRequest[] =
    "dmap_server_discovery_unlock_request";

ServerDiscovery::~ServerDiscovery() {}

void ServerDiscovery::announce() { CHECK(requestAck<kAnnounceRequest>()); }

int ServerDiscovery::getPeers(std::vector<PeerId>* peers) {
  CHECK_NOTNULL(peers);
  Message response_message;
  request<kGetPeersRequest>(&response_message);
  CHECK(response_message.isType<kGetPeersResponse>());
  proto::ServerDiscoveryGetPeersResponse response;
  response_message.extract<kGetPeersResponse>(&response);
  for (int i = 0; i < response.peers_size(); ++i) {
    peers->push_back(PeerId(response.peers(i)));
  }
  return response.peers_size();
}

void ServerDiscovery::lock() {
  while (!requestAck<kLockRequest>()) {
    usleep(1000);
  }
}

void ServerDiscovery::remove(const PeerId& peer) {
  Message request, response;
  request.impose<kRemoveRequest>(peer.ipPort());
  server_.request(&request, &response);
  CHECK(response.isType<Message::kAck>());
}

void ServerDiscovery::unlock() { CHECK(requestAck<kUnlockRequest>()); }

ServerDiscovery::ServerDiscovery(const PeerId& address, zmq::context_t& context)
    : server_(address, context, ZMQ_REQ) {}

} /* namespace dmap */
