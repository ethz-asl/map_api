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

void Peer::deleteFunction(Peer* peer_pointer) {
  delete peer_pointer;
}

bool Peer::request(const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  int size = request.ByteSize();
  void* buffer = malloc(size);
  request.SerializeToArray(buffer, size);
  zmq::message_t message(buffer, size, NULL, NULL);
  socket_.send(message);
  socket_.recv(&message);
  CHECK(response->ParseFromArray(message.data(), message.size()));
  return true;
}

}
// namespace map_api
