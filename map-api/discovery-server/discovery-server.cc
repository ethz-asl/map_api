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

#include <string>
#include <unordered_set>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "./core.pb.h"
#include <map-api/logical-time.h>
#include <map-api/message.h>
#include <map-api/peer-id.h>
#include <map-api/server-discovery.h>

using namespace map_api;  // NOLINT

DEFINE_string(ip_port, "127.0.0.1:5050", "Address to be used");

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  std::unordered_set<PeerId> peers_;
  bool locked = false;
  PeerId locker;

  zmq::context_t context;
  zmq::socket_t server(context, ZMQ_REP);
  server.bind(("tcp://" + FLAGS_ip_port).c_str());

  while (true) {
    zmq::message_t request;
    server.recv(&request);
    Message query, response;
    if (!query.ParseFromArray(request.data(), request.size())) {
      LOG(ERROR) << "Received a invalid message, discarding!";
      server.send(request);  // ZMQ_REP socket must reply to every request
      continue;
    }
    LogicalTime::synchronize(LogicalTime(query.logical_time()));

    if (!query.isType<ServerDiscovery::kLockRequest>()) {
      CHECK(locked && locker == query.sender());
    }

    if (query.isType<ServerDiscovery::kAnnounceRequest>()) {
      peers_.insert(PeerId(query.sender()));
      LOG(INFO) << query.sender() << " joined";
      response.ack();
    } else if (query.isType<ServerDiscovery::kGetPeersRequest>()) {
      proto::ServerDiscoveryGetPeersResponse get_peers_response;
      for (const PeerId& peer : peers_) {
        get_peers_response.add_peers(peer.ipPort());
      }
      response.impose<ServerDiscovery::kGetPeersResponse>(get_peers_response);
    } else if (query.isType<ServerDiscovery::kLockRequest>()) {
      if (locked) {
        response.decline();
      } else {
        locked = true;
        locker = query.sender();
        response.ack();
      }
    } else if (query.isType<ServerDiscovery::kRemoveRequest>()) {
      std::string to_remove;
      query.extract<ServerDiscovery::kRemoveRequest>(&to_remove);
      peers_.erase(PeerId(to_remove));
      LOG(INFO) << query.sender() << " removed " << to_remove;
      response.ack();
    } else if (query.isType<ServerDiscovery::kUnlockRequest>()) {
      CHECK(locked && locker == query.sender());
      locked = false;
      response.ack();
    } else {
      LOG(FATAL) << "Unknown request type for discovery server";
    }
    response.set_logical_time(LogicalTime::sample().serialize());
    response.set_sender(FLAGS_ip_port);
    std::string serialized_response = response.SerializeAsString();
    zmq::message_t response_message(serialized_response.size());
    memcpy(reinterpret_cast<void*>(response_message.data()),
           serialized_response.c_str(), serialized_response.size());
    server.send(response_message);
  }
  return 0;
}
