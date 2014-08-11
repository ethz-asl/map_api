#include "map-api/peer-id.h"

#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <Poco/RegularExpression.h>

#include "map-api/map-api-hub.h"

namespace map_api {

PeerId::PeerId() : ip_port_(kInvalidAdress) {}

PeerId::PeerId(const std::string& ip_port) : ip_port_(ip_port) {
  // TOOD(tcies): The regex does not match on 14.04.
  Poco::RegularExpression re("^\\d+\\.\\d+\\.\\d+\\.\\d+\\:\\d+$");
  if (!re.match(ip_port)) {
    VLOG(3) << "Invalid address " << ip_port;
  }
}

PeerId& PeerId::operator =(const PeerId& other) {
  ip_port_ = other.ip_port_;
  return *this;
}

PeerId PeerId::self() {
  return PeerId(MapApiHub::instance().ownAddress());
}

size_t PeerId::selfRank() {
  PeerId self_id = self();
  std::set<PeerId> peers;
  MapApiHub::instance().getPeers(&peers);
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
