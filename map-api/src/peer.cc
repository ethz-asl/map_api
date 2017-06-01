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

#include "map-api/peer.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "map-api/hub.h"
#include "map-api/internal/network-data-log.h"
#include "map-api/logical-time.h"
#include "map-api/message.h"
#include "map-api/peer-id.h"

DEFINE_int32(request_timeout, 10000,
             "Amount of milliseconds after which a "
             "non-responsive peer is considered disconnected");
DEFINE_int32(socket_linger_ms, 0,
             "Amount of milliseconds for which a socket "
             "waits for outgoing messages to process before closing.");
DEFINE_int32(simulated_lag_ms, 0,
             "Duration in milliseconds of the simulated lag.");
DEFINE_int32(simulated_bandwidth_kbps, 0,
             "Simulated bandwidth in kB/s. 0 means infinite.");

namespace map_api {

namespace peer_internal {
void customFree (void *data, void *hint)
{
    free (data);
}
}  // namespace peer_internal

Peer::Peer(const PeerId& address, zmq::context_t& context, int socket_type)
    : address_(address), socket_(context, socket_type) {
  std::lock_guard<std::mutex> lock(socket_mutex_);
  try {
    const int linger_ms = FLAGS_socket_linger_ms;
    socket_.setsockopt(ZMQ_LINGER, &linger_ms, sizeof(linger_ms));
    socket_.connect(("tcp://" + address.ipPort()).c_str());
  }
  catch (const std::exception& e) {  // NOLINT
    LOG(FATAL) << "Connection to " << address << " failed";
  }
}

const PeerId& Peer::address() const { return address_; }

void Peer::request(Message* request, Message* response) {
  CHECK_NOTNULL(request);
  CHECK_NOTNULL(response);
  CHECK(try_request(request, response)) << "Message " << request->DebugString()
                                        << " timed out!";
}

bool Peer::try_request(Message* request, Message* response) {
  return try_request_for(FLAGS_request_timeout, request, response);
}

bool Peer::try_request_for(int timeout_ms, Message* request,
                           Message* response) {
  CHECK_NOTNULL(request);
  CHECK_NOTNULL(response);
  request->set_sender(PeerId::self().ipPort());
  request->set_logical_time(LogicalTime::sample().serialize());
  int size = request->ByteSize();
  VLOG(3) << "Message size is " << size;
  void* buffer = malloc(size);
  CHECK(request->SerializeToArray(buffer, size));
  try {
    zmq::message_t message(buffer, size, peer_internal::customFree, NULL);
    {
      std::lock_guard<std::mutex> lock(socket_mutex_);

      usleep(1e3 * FLAGS_simulated_lag_ms);
      Hub::instance().logOutgoing(size, request->type());

      simulateBandwidth(message.size());
      socket_.setsockopt(ZMQ_RCVTIMEO, &timeout_ms, sizeof(timeout_ms));
      CHECK(socket_.send(message));
      if (!socket_.recv(&message)) {
        LOG(WARNING) << "Try-request of type " << request->type()
                     << " failed for peer " << address_;
        return false;
      }
    }
    // catches silly bugs where a handler forgets to modify the response
    // message, which could be a quite common bug
    CHECK_GT(message.size(), 0u) << "Request was " << request->DebugString();
    CHECK(response->ParseFromArray(message.data(), message.size()));
    Hub::instance().logIncoming(message.size(), response->type());
    LogicalTime::synchronize(LogicalTime(response->logical_time()));
  }
  catch (const zmq::error_t& e) {
    LOG(FATAL) << e.what() << ", request was " << request->DebugString()
               << ", sent to " << address_;
  }
  return true;
}

void Peer::simulateBandwidth(size_t byte_size) {
  if (FLAGS_simulated_bandwidth_kbps == 0) {
    return;
  }
  usleep(1000 * byte_size / FLAGS_simulated_bandwidth_kbps);
}

}  // namespace map_api
