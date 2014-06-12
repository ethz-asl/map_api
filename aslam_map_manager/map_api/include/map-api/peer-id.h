#ifndef MAP_API_PEER_ID_H_
#define MAP_API_PEER_ID_H_

#include <iostream>
#include <string>

namespace map_api {

class PeerId {
 public:
  explicit PeerId(const std::string& ip_port);

  const std::string& ipPort() const;

  bool operator ==(const PeerId& other) const;

 private:
  std::string ip_port_;
};

} /* namespace map_api */

namespace std {

inline ostream& operator<<(ostream& out, const map_api::PeerId& peer_id) {
  out << "IpPort(" << peer_id.ipPort() << ")";
  return out;
}

template<>
struct hash<map_api::PeerId>{
  std::size_t operator()(const map_api::PeerId& peer_id) const {
    return std::hash<std::string>()(peer_id.ipPort());
  }
};

} /* namespace std */

#endif /* MAP_API_PEER_ID_H_ */
