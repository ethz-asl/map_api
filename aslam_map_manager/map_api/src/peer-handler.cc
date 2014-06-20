#include "map-api/peer-handler.h"

#include <glog/logging.h>

#include "map-api/map-api-hub.h"

namespace map_api {

void PeerHandler::add(const PeerId& peer) {
  peers_.insert(peer);
}

void PeerHandler::broadcast(
    const Message& request,
    std::unordered_map<PeerId, Message>* responses) {
  CHECK_NOTNULL(responses);
  responses->clear();
  // TODO(tcies) parallelize using std::future
  for (const PeerId& peer: peers_) {
    MapApiHub::instance().request(peer, request, &(*responses)[peer]);
  }
}

const std::set<PeerId>& PeerHandler::peers() const {
  return peers_;
}

void PeerHandler::request(
    const PeerId& peer, const Message& request,
    Message* response) {
  CHECK_NOTNULL(response);
  std::set<PeerId>::iterator found = peers_.find(peer);
  if (found == peers_.end()) {
    found = peers_.insert(peer).first;
  }
  MapApiHub::instance().request(peer, request, response);
}

size_t PeerHandler::size() const {
  return peers_.size();
}

bool PeerHandler::forwardOrderSerialBroadcast(const Message& request) {
  if (!peers_.size()) return true;
  Message response;
  std::set<PeerId>::iterator it = peers_.begin();
  MapApiHub::instance().request(*it, request, &response);
  if (!response.isType<Message::kAck>()) {
    return false;
  }
  for (++it; it != peers_.end(); ++it) {
    MapApiHub::instance().request(*it, request, &response);
    CHECK(response.isType<Message::kAck>());
  }
  return true;
}

bool PeerHandler::reverseOrderSerialBroadcast(const Message& request) {
  if (!peers_.size()) return true;
  Message response;
  std::set<PeerId>::reverse_iterator it = peers_.rbegin();
  MapApiHub::instance().request(*it, request, &response);
  if (!response.isType<Message::kAck>()) {
    return false;
  }
  for (++it; it != peers_.rend(); ++it) {
    MapApiHub::instance().request(*it, request, &response);
    CHECK(response.isType<Message::kAck>());
  }
  return true;
}

bool PeerHandler::undisputableBroadcast(
    const Message& request) {
  std::unordered_map<PeerId, Message> responses;
  broadcast(request, &responses);
  for (const std::pair<PeerId, Message>& response_pair : responses) {
    if (!response_pair.second.isType<Message::kAck>()){
      return false;
    }
  }
  return true;
}

} /* namespace map_api */
