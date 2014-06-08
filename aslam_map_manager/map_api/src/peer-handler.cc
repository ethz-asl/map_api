#include "map-api/peer-handler.h"

#include "map-api/map-api-hub.h"

namespace map_api {

template <>
std::shared_ptr<Peer> PeerHandler<std::shared_ptr<Peer> >::lock(
    const std::shared_ptr<Peer>& pointer) const {
  return pointer;
}
template <>
std::shared_ptr<Peer> PeerHandler<std::weak_ptr<Peer> >::lock(
    const std::weak_ptr<Peer>& pointer) const {
  return pointer.lock();
}

template <>
void PeerHandler<std::weak_ptr<Peer> >::insert(
    std::weak_ptr<Peer> peer_pointer) {
  std::shared_ptr<Peer> locked_pointer = peer_pointer.lock();
  CHECK(locked_pointer);
  CHECK(peers_.insert(
      std::make_pair(locked_pointer->address(), peer_pointer)).second);
}

template <>
void PeerHandler<std::shared_ptr<Peer> >::insert(
    const std::string& address, zmq::context_t& context, int socket_type) {
  std::shared_ptr<Peer> to_insert =
      std::shared_ptr<Peer>(new Peer(address, context, socket_type),
                            Peer::deleteFunction);
  CHECK(peers_.insert(std::make_pair(address, to_insert)).second);
}

template <>
std::weak_ptr<Peer> PeerHandler<std::weak_ptr<Peer> >::ensure(
    const std::string& address) {
  std::unordered_map<std::string, std::weak_ptr<Peer> >::iterator found =
      peers_.find(address);
  if (found == peers_.end()) {
    // TODO(slynen) in some sense, it would be cleaner for the weak flavor of
    // PeerHandler to have a reference/pointer to its corresponding strong
    // flavor object, instead of this hardcoded reference to MapApiHub. Wdyt?
    std::weak_ptr<Peer> requested_peer = MapApiHub::instance().ensure(address);
    CHECK(!requested_peer.expired());
    peers_[address] = requested_peer;
    return requested_peer;
  }
  return found->second;
}

template <>
std::weak_ptr<Peer> PeerHandler<std::shared_ptr<Peer> >::ensure(
    const std::string& address) {
  std::unordered_map<std::string, std::shared_ptr<Peer> >::iterator found =
      peers_.find(address);
  if (found == peers_.end()) {
    // another fantastic display of the design flaw of PeerHandler
    zmq::context_t* context;
    int socket_type;
    MapApiHub::instance().getContextAndSocketType(&context, &socket_type);
    insert(address, *context, socket_type);
    found = peers_.find(address);
    CHECK(found != peers_.end());
  }
  return std::weak_ptr<Peer>(found->second);
}

} /* namespace map_api */
