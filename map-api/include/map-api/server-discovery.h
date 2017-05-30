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
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

#ifndef MAP_API_SERVER_DISCOVERY_H_
#define MAP_API_SERVER_DISCOVERY_H_

#include <string>
#include <vector>

#include <glog/logging.h>

#include <zeromq_cpp/zmq.hpp>

#include "map-api/discovery.h"
#include "map-api/message.h"
#include "map-api/peer.h"

namespace map_api {
class PeerId;

/**
 * Regulates discovery through /tmp/mapapi-discovery.txt .
 */
class ServerDiscovery final : public Discovery {
 public:
  virtual ~ServerDiscovery();
  virtual void announce() final override;
  virtual int getPeers(std::vector<PeerId>* peers) final override;
  virtual void lock() final override;
  virtual void remove(const PeerId& peer) final override;
  virtual void unlock() final override;

  static const char kAnnounceRequest[];
  static const char kGetPeersRequest[];
  static const char kGetPeersResponse[];
  static const char kLockRequest[];
  static const char kRemoveRequest[];
  static const char kUnlockRequest[];

 private:
  /**
   * May only be used by the Hub
   */
  ServerDiscovery(const PeerId& address, zmq::context_t& context);
  ServerDiscovery(const ServerDiscovery&) = delete;
  ServerDiscovery& operator=(const ServerDiscovery&) = delete;
  friend class Hub;

  template <const char* request_type>
  void request(Message* response) {
    CHECK_NOTNULL(response);
    Message request;
    request.impose<request_type>();
    server_.request(&request, response);
  }

  template <const char* request_type>
  bool requestAck() {
    Message response;
    request<request_type>(&response);
    return response.isType<Message::kAck>();
  }

  Peer server_;
};

} // namespace map_api

#endif  // MAP_API_SERVER_DISCOVERY_H_
