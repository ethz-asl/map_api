#ifndef MAP_API_PEER_HANDLER_H_
#define MAP_API_PEER_HANDLER_H_

#include <memory>
#include <vector>
#include <unordered_map>
#include <string>

#include "map-api/message.h"
#include "map-api/peer.h"

namespace map_api {

/**
 * Allows to hold shared or weak pointers to peers and exposes common operations
 * on the peers. TODO(tcies) synchronization
 */
template <typename PeerPointerType>
class PeerHandler {
 public:
  /**
   * Sends the message to all peers and collects their responses
   */
  void broadcast(const Message& request,
                 std::unordered_map<std::string, Message>* responses);
  /**
   * TODO(tcies) later: Notify about disconnection
   */
  void clear();
  /**
   * Returns true if all peers have acknowledged, false otherwise.
   * TODO(tcies) timeouts?
   */
  bool undisputable_broadcast(const Message& request);

  /**
   * Inserts peer obtained from other PeerHandler. Should be only called for
   * weak pointer flavor of PeerHandler: Each peer should only have one shared
   * pointer, and that shared pointer should be inserted with the other
   * insert() overload
   */
  void insert(PeerPointerType peer_pointer);
  void insert(const std::string& address, zmq::context_t& context,
              int socket_type);

  size_t size() const;
 private:
  std::shared_ptr<Peer> lock(const PeerPointerType& peer) const;

  std::unordered_map<std::string, PeerPointerType> peers_;
};

} /* namespace map_api */

#include "map-api/peer-handler-inl.h"

#endif /* MAP_API_PEER_HANDLER_H_ */
