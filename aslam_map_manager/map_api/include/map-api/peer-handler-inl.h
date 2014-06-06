#ifndef MAP_API_PEER_HANDLER_INL_H_
#define MAP_API_PEER_HANDLER_INL_H_

#include <glog/logging.h>

namespace map_api {

template <typename PeerPointerType>
void PeerHandler<PeerPointerType>::broadcast(
    const Message& request,
    std::unordered_map<std::string, Message>* responses) {
  CHECK_NOTNULL(responses);
  responses->clear();
  // TODO(tcies) parallelize
  for (const std::pair<std::string, PeerPointerType>& peer_pair : peers_) {
    std::shared_ptr<Peer> shared_peer = this->lock(peer_pair.second);
    CHECK(shared_peer);
    shared_peer->request(request, &(*responses)[peer_pair.first]);
  }
}

template <typename PeerPointerType>
void PeerHandler<PeerPointerType>::clear() {
  this->peers_.clear();
}

template <typename PeerPointerType>
size_t PeerHandler<PeerPointerType>::size() const {
  return this->peers_.size();
}

template <typename PeerPointerType>
bool PeerHandler<PeerPointerType>::undisputable_broadcast(
    const Message& request) {
  std::unordered_map<std::string, Message> responses;
  broadcast(request, &responses);
  for (const std::pair<std::string, Message>& response_pair : responses) {
    if (!response_pair.second.isType<Message::kAck>()){
      return false;
    }
  }
  return true;
}

} // namespace map_api


#endif /* MAP_API_PEER_HANDLER_INL_H_ */
