#include "map-api/peer-handler.h"

namespace map_api {

template <>
std::shared_ptr<Peer> PeerHandler<std::shared_ptr<Peer>>::lock(
    const std::shared_ptr<Peer>& pointer) const {
  return pointer;
}
template <>
std::shared_ptr<Peer> PeerHandler<std::weak_ptr<Peer>>::lock(
    const std::weak_ptr<Peer>& pointer) const {
  return pointer.lock();
}

template <>
void PeerHandler<std::weak_ptr<Peer>>::insert(
    std::weak_ptr<Peer> peer_pointer) {
  std::shared_ptr<Peer> locked_pointer = peer_pointer.lock();
  CHECK(locked_pointer);
  CHECK(peers_.insert(
      std::make_pair(locked_pointer->address(), peer_pointer)).second);
}
template <>
void PeerHandler<std::shared_ptr<Peer>>::insert(
    std::shared_ptr<Peer> peer_pointer) {
  LOG(FATAL) << "This flavor of insert should never be called!";
}
template <>
void PeerHandler<std::weak_ptr<Peer>>::insert(
    const std::string& address, zmq::context_t& context, int socket_type) {
  LOG(FATAL) << "This flavor of insert should never be called!";
}
template <>
void PeerHandler<std::shared_ptr<Peer>>::insert(
    const std::string& address, zmq::context_t& context, int socket_type) {
  std::shared_ptr<Peer> to_insert =
      std::shared_ptr<Peer>(new Peer(address, context, socket_type),
                            Peer::deleteFunction);
  CHECK(peers_.insert(std::make_pair(address, to_insert)).second);
}


} /* namespace map_api */
