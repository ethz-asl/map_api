#ifndef MAP_API_PEER_H_
#define MAP_API_PEER_H_

#include <mutex>

#include <zeromq_cpp/zmq.hpp>

#include "map-api/message.h"

namespace map_api {

class Peer {
 public:
  std::string address() const;

  void request(const Message& request, Message* response);

  /**
   * Unlike request, doesn't terminate if the request times out.
   */
  bool try_request(const Message& request, Message* response);

 private:
  /**
   * Life cycle management of Peer objects reserved for MapApiHub.
   */
  friend class MapApiHub;
  explicit Peer(const std::string& address, zmq::context_t& context,
                int socket_type);
  /**
   * Closes the peer socket.
   */
  bool disconnect();

  // ZMQ sockets are not inherently thread-safe
  std::string address_;
  zmq::socket_t socket_;
  std::mutex socket_mutex_;
};

} // namespace map_api

#endif /* MAP_API_PEER_H_ */
