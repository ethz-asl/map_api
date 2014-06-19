#include "map-api/peer-id.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

DECLARE_string(ip_port);

namespace map_api {

PeerId::PeerId() : ip_port_("") {}

PeerId::PeerId(const std::string& ip_port) : ip_port_(ip_port) {}

PeerId& PeerId::operator =(const PeerId& other) {
  ip_port_ = other.ip_port_;
  return *this;
}

PeerId PeerId::self() {
  return PeerId(FLAGS_ip_port);
}

const std::string& PeerId::ipPort() const {
  return ip_port_;
}

bool PeerId::operator ==(const PeerId& other) const {
  CHECK(isValid());
  return ip_port_ == other.ip_port_;
}

bool PeerId::isValid() const {
  return ip_port_ != "";
}

} /* namespace map_api */
