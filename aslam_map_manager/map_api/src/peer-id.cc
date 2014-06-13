#include "map-api/peer-id.h"

namespace map_api {

PeerId::PeerId(const std::string& ip_port) : ip_port_(ip_port) {}

const std::string& PeerId::ipPort() const {
  return ip_port_;
}

bool PeerId::operator ==(const PeerId& other) const {
  return ip_port_ == other.ip_port_;
}

} /* namespace map_api */
