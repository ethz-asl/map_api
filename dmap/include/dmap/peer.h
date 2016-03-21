#ifndef DMAP_PEER_H_
#define DMAP_PEER_H_

#include <memory>
#include <mutex>
#include <string>

#include <zeromq_cpp/zmq.hpp>

#include "dmap/peer-id.h"

namespace dmap {
class Message;

namespace internal {
class NetworkDataLog;
}  // namespace internal

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

  static std::unique_ptr<internal::NetworkDataLog> outgoing_log_;
};

}  // namespace dmap

#endif  // DMAP_PEER_H_
