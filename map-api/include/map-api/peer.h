#ifndef DMAP_PEER_H_
#define DMAP_PEER_H_

#include <memory>
#include <mutex>
#include <string>

#include <zeromq_cpp/zmq.hpp>

#include "map-api/message.h"
#include "map-api/peer-id.h"

namespace map_api {

namespace internal {
class NetworkDataLog;
}  // namespace internal

class Peer {
 public:
  explicit Peer(const PeerId& address, zmq::context_t& context,
                int socket_type);

  const PeerId& address() const;

  void request(Message* request, Message* response);
  // Requires specification of Message::UniqueType. This specialization is
  // included in the DMAP_UNIQUE_PROTO_MESSAGE macro in message.h.
  template <typename RequestType, typename ResponseType>
  void request(const RequestType& request, ResponseType* response) {
    CHECK_NOTNULL(response);
    Message request_message, response_message;
    request_message.impose<Message::UniqueType<RequestType>::message_name>(
        request);
    this->request(&request_message, &response_message);
    response_message.extract<Message::UniqueType<ResponseType>::message_name>(
        response);
  }

  /**
   * Unlike request, doesn't terminate if the request times out.
   */
  bool try_request(Message* request, Message* response);
  bool try_request_for(int timeout_ms, Message* request, Message* response);

  static void simulateBandwidth(size_t byte_size);

 private:
  // ZMQ sockets are not inherently thread-safe
  PeerId address_;
  zmq::socket_t socket_;
  std::mutex socket_mutex_;

  static std::unique_ptr<internal::NetworkDataLog> outgoing_log_;
};

}  // namespace map_api

#endif  // DMAP_PEER_H_
