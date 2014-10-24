#ifndef MAP_API_PEER_ID_H_
#define MAP_API_PEER_ID_H_

#include <iostream>  // NOLINT
#include <string>

namespace map_api {

class PeerId {
 public:
  PeerId();

  explicit PeerId(const std::string& ip_port);

  /**
   * checks whether serialized PeerId string is valid
   */
  static bool isValid(const std::string& serialized);

  PeerId& operator =(const PeerId& other);

  static PeerId self();

  /**
   * Rank compared to other peers in network.
   */
  static size_t selfRank();

  const std::string& ipPort() const;

  bool operator <(const PeerId& other) const;

  bool operator ==(const PeerId& other) const;

  bool operator !=(const PeerId& other) const;

  bool isValid() const;

 private:
  static const std::string kInvalidAdress;

  std::string ip_port_;
};

} /* namespace map_api */

namespace std {

inline ostream& operator<<(ostream& out, const map_api::PeerId& peer_id) {
  out << "IpPort(" << peer_id.ipPort() << ")";
  return out;
}

template <>
struct hash<map_api::PeerId> {
  std::size_t operator()(const map_api::PeerId& peer_id) const {
    return std::hash<std::string>()(peer_id.ipPort());
  }
};

} /* namespace std */

#endif  // MAP_API_PEER_ID_H_
