#include "map-api/peer-handler.h"

#include <glog/logging.h>

#include "map-api/hub.h"

namespace map_api {

void PeerHandler::add(const PeerId& peer) {
  peers_.insert(peer);
}

void PeerHandler::broadcast(
    Message* request, std::unordered_map<PeerId, Message>* responses) {
  CHECK_NOTNULL(request);
  CHECK_NOTNULL(responses);
  responses->clear();
  // TODO(tcies) parallelize using std::future
  for (const PeerId& peer: peers_) {
    Hub::instance().request(peer, request, &(*responses)[peer]);
  }
}

bool PeerHandler::empty() const {
  return peers_.empty();
}

const std::set<PeerId>& PeerHandler::peers() const {
  return peers_;
}

void PeerHandler::remove(const PeerId& peer) {
  std::set<PeerId>::iterator found = peers_.find(peer);
  if (found == peers_.end()) {
    std::stringstream report;
    report << "Removing peer " << peer << " failed. Peers are:" << std::endl;
    for (const PeerId& existing : peers_) {
      report << existing << ", ";
    }
    LOG(FATAL) << report.str();
  }
  peers_.erase(peer);
}

void PeerHandler::request(
    const PeerId& peer, Message* request,
    Message* response) {
  CHECK_NOTNULL(request);
  CHECK_NOTNULL(response);
  std::set<PeerId>::iterator found = peers_.find(peer);
  if (found == peers_.end()) {
    found = peers_.insert(peer).first;
  }
  Hub::instance().request(peer, request, response);
}

bool PeerHandler::try_request(const PeerId& peer, Message* request,
                              Message* response) {
  CHECK_NOTNULL(request);
  CHECK_NOTNULL(response);
  CHECK_NE(peer, PeerId::self());
  std::set<PeerId>::iterator found = peers_.find(peer);
  if (found == peers_.end()) {
    found = peers_.insert(peer).first;
  }
  return Hub::instance().try_request(peer, request, response);
}

size_t PeerHandler::size() const {
  return peers_.size();
}

bool PeerHandler::undisputableBroadcast(Message* request) {
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
