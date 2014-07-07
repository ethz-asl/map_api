#ifndef MAP_API_PEER_HANDLER_H_
#define MAP_API_PEER_HANDLER_H_

#include <memory>
#include <vector>
#include <unordered_map>
#include <set>
#include <string>

#include "map-api/message.h"
#include "map-api/peer.h"
#include "map-api/peer-id.h"

namespace map_api {

/**
 * Allows to hold identifiers of peers and exposes common operations
 * on the peers, querying MapApiHub
 */
class PeerHandler {
 public:
  void add(const PeerId& peer);
  /**
   * Sends the message to all currently connected peers and collects their
   * responses
   */
  void broadcast(Message* request,
                 std::unordered_map<PeerId, Message>* responses);

  bool empty() const;
  /**
   * Allows user to view peers, e.g. for ConnectResponse
   * TODO(simon) is this cheap? What else to fill ConnectResponse with
   * addresses?
   */
  const std::set<PeerId>& peers() const;

  void remove(const PeerId& peer);
  /**
   * Sends request to specified peer. If peer not among peers_, adds it.
   */
  void request(const PeerId& peer_address, Message* request,
               Message* response);
  /**
   * Returns true if all peers have acknowledged, false otherwise.
   * TODO(tcies) timeouts?
   */
  bool undisputableBroadcast(Message* request);

  size_t size() const;
 private:
  std::set<PeerId> peers_; // std::set to ensure uniform ordering
};

} /* namespace map_api */

#endif /* MAP_API_PEER_HANDLER_H_ */
