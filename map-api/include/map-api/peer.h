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

#ifndef MAP_API_PEER_H_
#define MAP_API_PEER_H_

#include <memory>
#include <mutex>
#include <string>

#include <zeromq_cpp/zmq.hpp>

#include "map-api/message.h"
#include "map-api/peer-id.h"

namespace map_api {

namespace internal {
class NetworkDataLog;
}  // namespace internal

class Peer {
 public:
  explicit Peer(const PeerId& address, zmq::context_t& context,
                int socket_type);

  const PeerId& address() const;

  void request(Message* request, Message* response);
  // Requires specification of Message::UniqueType. This specialization is
  // included in the MAP_API_UNIQUE_PROTO_MESSAGE macro in message.h.
  template <typename RequestType, typename ResponseType>
  void request(const RequestType& request, ResponseType* response) {
    CHECK_NOTNULL(response);
    Message request_message, response_message;
    request_message.impose<Message::UniqueType<RequestType>::message_name>(
        request);
    this->request(&request_message, &response_message);
    response_message.extract<Message::UniqueType<ResponseType>::message_name>(
        response);
  }

  /**
   * Unlike request, doesn't terminate if the request times out.
   */
  bool try_request(Message* request, Message* response);
  bool try_request_for(int timeout_ms, Message* request, Message* response);

  static void simulateBandwidth(size_t byte_size);

 private:
  // ZMQ sockets are not inherently thread-safe
  PeerId address_;
  zmq::socket_t socket_;
  std::mutex socket_mutex_;

  static std::unique_ptr<internal::NetworkDataLog> outgoing_log_;
};

}  // namespace map_api

#endif  // MAP_API_PEER_H_
