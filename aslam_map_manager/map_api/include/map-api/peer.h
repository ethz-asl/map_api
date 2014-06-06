#ifndef MAP_API_PEER_H_
#define MAP_API_PEER_H_

#include <zeromq_cpp/zmq.hpp>

#include "map-api/message.h"

namespace map_api {

class Peer {
 public:
  std::string address() const;

  bool request(const Message& request, Message* response);

  /**
   * Peer delete function that can be passed to a shared pointer constructor,
   * otherwise can't make shared pointers of peers with private-ization of
   * CTOR/DTOR below.
   * http://stackoverflow.com/questions/8202530
   */
  static void deleteFunction(Peer* peer_pointer);

 private:
  /**
   * Life cycle management of Peer objects reserved for PeerHandler
   */
  template <typename PeerPointerType>
  friend class PeerHandler;
  explicit Peer(const std::string& address, zmq::context_t& context,
                int socket_type);
  Peer(const Peer&) = default;
  Peer& operator=(const Peer&) = default;
  ~Peer() = default;

  std::string address_;
  zmq::socket_t socket_;
};

} // namespace map_api

#endif /* MAP_API_PEER_H_ */
