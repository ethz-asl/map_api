#include "map-api/peer.h"

#include <glog/logging.h>

namespace map_api {

Peer::Peer(const std::string& address, zmq::context_t& context,
           int socket_type)
: address_(address), socket_(context, socket_type) {
  //TODO(tcies) init instead of aborting constructor
  try {
    socket_.connect(("tcp://" + address).c_str());
  } catch (const std::exception& e) {
    LOG(FATAL) << "Connection to " << address << " failed";
  }
}

std::string Peer::address() const {
  return address_;
}

bool Peer::request(const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  int size = request.ByteSize();
  void* buffer = malloc(size);
  CHECK(request.SerializeToArray(buffer, size));
  try {
    zmq::message_t message(buffer, size, NULL, NULL);
    CHECK(socket_.send(message));
    CHECK(socket_.recv(&message));
    CHECK(response->ParseFromArray(message.data(), message.size()));
  } catch(const zmq::error_t& e) {
    LOG(FATAL) << e.what();
  }
  return true;
}

}
// namespace map_api
