// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API. If not, see <http://www.gnu.org/licenses/>.

#include <map-api/server-discovery.h>
#include <fstream>  // NOLINT
#include <sstream>  // NOLINT
#include <string>

#include <sys/file.h>  // linux-specific

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "map-api/hub.h"
#include "./core.pb.h"

namespace map_api {

const char ServerDiscovery::kAnnounceRequest[] =
    "map_api_server_discovery_announce_request";
const char ServerDiscovery::kGetPeersRequest[] =
    "map_api_server_discovery_get_peers_request";
const char ServerDiscovery::kGetPeersResponse[] =
    "map_api_server_discovery_get_peers_response";
MAP_API_PROTO_MESSAGE(ServerDiscovery::kGetPeersResponse,
                   proto::ServerDiscoveryGetPeersResponse);
const char ServerDiscovery::kLockRequest[] =
    "map_api_server_discovery_lock_request";
const char ServerDiscovery::kRemoveRequest[] =
    "map_api_server_discovery_remove_request";
MAP_API_STRING_MESSAGE(ServerDiscovery::kRemoveRequest);
const char ServerDiscovery::kUnlockRequest[] =
    "map_api_server_discovery_unlock_request";

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

} // namespace map_api
