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

#include <map-api/peer-handler.h>
#include <glog/logging.h>

#include "map-api/hub.h"
#include "map-api/message.h"

namespace map_api {

void PeerHandler::add(const PeerId& peer) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    peers_.insert(peer);
  }
  cv_.notify_all();
}

void PeerHandler::broadcast(Message* request,
                            std::unordered_map<PeerId, Message>* responses) {
  CHECK_NOTNULL(request);
  CHECK_NOTNULL(responses);
  responses->clear();
  std::lock_guard<std::mutex> lock(mutex_);
  for (const PeerId& peer : peers_) {
    Hub::instance().request(peer, request, &(*responses)[peer]);
  }
}

bool PeerHandler::empty() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return peers_.empty();
}

void PeerHandler::awaitNonEmpty(const std::string& info) const {
  std::unique_lock<std::mutex> lock(mutex_);
  while (peers_.empty()) {
    if (info != "") {
      LOG(INFO) << info;
    }
    cv_.wait(lock);
  }
}

const std::set<PeerId>& PeerHandler::peers() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return peers_;
}

void PeerHandler::remove(const PeerId& peer) {
  std::lock_guard<std::mutex> lock(mutex_);
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

void PeerHandler::request(const PeerId& peer, Message* request,
                          Message* response) {
  CHECK_NOTNULL(request);
  CHECK_NOTNULL(response);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    std::set<PeerId>::iterator found = peers_.find(peer);
    if (found == peers_.end()) {
      found = peers_.insert(peer).first;
    }
  }
  cv_.notify_all();
  Hub::instance().request(peer, request, response);
}

bool PeerHandler::try_request(const PeerId& peer, Message* request,
                              Message* response) {
  CHECK_NOTNULL(request);
  CHECK_NOTNULL(response);
  CHECK_NE(peer, PeerId::self());
  {
    std::lock_guard<std::mutex> lock(mutex_);
    std::set<PeerId>::iterator found = peers_.find(peer);
    if (found == peers_.end()) {
      found = peers_.insert(peer).first;
    }
  }
  cv_.notify_all();
  return Hub::instance().try_request(peer, request, response);
}

size_t PeerHandler::size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return peers_.size();
}

bool PeerHandler::undisputableBroadcast(Message* request) {
  std::unordered_map<PeerId, Message> responses;
  broadcast(request, &responses);
  for (const std::pair<PeerId, Message>& response_pair : responses) {
    if (!response_pair.second.isType<Message::kAck>()) {
      return false;
    }
  }
  return true;
}

} // namespace map_api
