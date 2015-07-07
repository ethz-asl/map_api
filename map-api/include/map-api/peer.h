#ifndef MAP_API_PEER_H_
#define MAP_API_PEER_H_

#include <mutex>
#include <string>

#include <gflags/gflags.h>
#include <zeromq_cpp/zmq.hpp>

#include "map-api/peer-id.h"

DECLARE_int32(request_timeout);

namespace map_api {
class Message;

class Peer {
 public:
  const PeerId& address() const;

  void request(Message* request, Message* response);

  /**
   * Unlike request, doesn't terminate if the request times out.
   */
  bool try_request(Message* request, Message* response);
  bool try_request_for(int timeout_ms, Message* request, Message* response);

  static void simulateBandwidth(size_t byte_size);

 private:
  /**
   * Life cycle management of Peer objects reserved for MapApiHub, with the
   * exception of server discovery.
   */
  friend class Hub;
  friend class ServerDiscovery;
  explicit Peer(const PeerId& address, zmq::context_t& context,
                int socket_type);

  // ZMQ sockets are not inherently thread-safe
  PeerId address_;
  zmq::socket_t socket_;
  std::mutex socket_mutex_;
};

}  // namespace map_api

#endif  // MAP_API_PEER_H_
