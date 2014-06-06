#ifndef MAP_API_PEER_H_
#define MAP_API_PEER_H_

#include <zeromq_cpp/zmq.hpp>

#include "core.pb.h"

namespace map_api {

class Peer {
 public:
  bool request(const proto::HubMessage& request,
               proto::HubMessage* response);

 private:
  zmq::socket_t* socket_;
};

} // namespace map_api

#endif /* MAP_API_PEER_H_ */
