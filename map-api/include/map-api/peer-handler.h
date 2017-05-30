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

#ifndef MAP_API_PEER_HANDLER_H_
#define MAP_API_PEER_HANDLER_H_

#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <set>
#include <string>

#include "map-api/peer-id.h"

namespace map_api {
class Message;

/**
 * Allows to hold identifiers of peers and exposes common operations
 * on the peers, querying MapApiHub
 */
class PeerHandler {
 public:
  void add(const PeerId& peer);
  /**
   * Sends the message to all currently connected peers and collects their
   * responses
   */
  void broadcast(Message* request,
                 std::unordered_map<PeerId, Message>* responses);

  bool empty() const;
  // If empty, print "info" (if != "") to LOG(INFO), then block until empty.
  void awaitNonEmpty(const std::string& info) const;
  /**
   * Allows user to view peers, e.g. for ConnectResponse
   * TODO(simon) is this cheap? What else to fill ConnectResponse with
   * addresses?
   */
  const std::set<PeerId>& peers() const;

  void remove(const PeerId& peer);
  /**
   * Sends request to specified peer. If peer not among peers_, adds it.
   */
  void request(const PeerId& peer_address, Message* request, Message* response);
  /**
   * Sends request to specified peer. If peer not among peers_, adds it. Returns
   * false on timeout.
   */
  bool try_request(const PeerId& peer_address, Message* request,
                   Message* response);
  /**
   * Returns true if all peers have acknowledged, false otherwise.
   * TODO(tcies) timeouts?
   */
  bool undisputableBroadcast(Message* request);

  size_t size() const;

 private:
  std::set<PeerId> peers_;  // std::set to ensure uniform ordering
  mutable std::mutex mutex_;
  mutable std::condition_variable cv_;
};

} // namespace map_api

#endif  // MAP_API_PEER_HANDLER_H_
