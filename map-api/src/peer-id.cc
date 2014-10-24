#include <map-api/peer-id.h>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <Poco/RegularExpression.h>

#include <map-api/hub.h>

namespace map_api {
PeerId::PeerId() : ip_port_(kInvalidAdress) {}

PeerId::PeerId(const std::string& ip_port) : ip_port_(ip_port) {
  CHECK(isValid(ip_port));
}

bool PeerId::isValid(const std::string& serialized) {
  size_t ip[4], port;
  bool success = true;
  if (sscanf(serialized.c_str(), "%lu.%lu.%lu.%lu:%lu",  // NOLINT
             &ip[0], &ip[1], &ip[2], &ip[3], &port) != 5) {
    success = false;
  }
  for (size_t i = 0u; i < 4; ++i) {
    if (ip[i] > 255) {
      success = false;
    }
  }
  if (port > 65535) {
    success = false;
  }
  return success;
}

PeerId& PeerId::operator =(const PeerId& other) {
  ip_port_ = other.ip_port_;
  return *this;
}

PeerId PeerId::self() { return PeerId(Hub::instance().ownAddress()); }

size_t PeerId::selfRank() {
  PeerId self_id = self();
  std::set<PeerId> peers;
  Hub::instance().getPeers(&peers);
  peers.insert(self_id);
  size_t i = 0;
  for (const PeerId& peer : peers) {
    if (peer == self_id) {
      return i;
    }
    ++i;
  }
  CHECK(false) << "Self not found in set!";
  return 0u;
}

const std::string& PeerId::ipPort() const {
  return ip_port_;
}

bool PeerId::operator <(const PeerId& other) const {
  return ip_port_ < other.ip_port_;
}

bool PeerId::operator ==(const PeerId& other) const {
  CHECK(isValid());
  return ip_port_ == other.ip_port_;
}

bool PeerId::operator !=(const PeerId& other) const {
  CHECK(isValid());
  return ip_port_ != other.ip_port_;
}

bool PeerId::isValid() const {
  return ip_port_ != kInvalidAdress;
}

const std::string PeerId::kInvalidAdress = "";

} /* namespace map_api */
