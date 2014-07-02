#ifndef MAP_API_HUB_H_
#define MAP_API_HUB_H_

#include <cstddef>
#include <functional>
#include <string>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <set>
#include <unordered_map>

#include <Poco/RWLock.h>
#include <zeromq_cpp/zmq.hpp>

#include "map-api/discovery.h"
#include "map-api/message.h"
#include "map-api/peer.h"
#include "map-api/peer-id.h"
#include "core.pb.h"

namespace map_api {

/**
 * Map Api Hub: Manages connections to other participating nodes
 */
class MapApiHub final {
 public:
  /**
   * Get singleton instance of Map Api Hub
   */
  static MapApiHub& instance();
  /**
   * Initialize hub
   */
  bool init();
  /**
   * Re-enter server thread, disconnect from peers, leave discovery
   */
  void kill();
  /**
   * Same as request(), but expects Message::kAck as response and returns false
   * if something else is received
   */
  bool ackRequest(const PeerId& peer, Message* request);
  /**
   * Lists the addresses of connected peers, ordered set for user convenience
   */
  void getPeers(std::set<PeerId>* destination) const;
  /**
   * Get amount of peers
   */
  int peerSize();

  const std::string& ownAddress() const;

  /**
   * Registers a handler for messages titled with the given name
   * TODO(tcies) create a metatable directory for these types as well
   * The handler must take two arguments: A string which contains the
   * serialized data to be treated and a socket pointer to which a message MUST
   * be sent at the end of the handler
   * TODO(tcies) distinguish between pub/sub and rpc
   */
  bool registerHandler(const char* type,
                       std::function<void(const Message& request,
                                          Message* response)> handler);
  /**
   * Sends out the specified message to all connected peers
   */
  void broadcast(Message* request,
                 std::unordered_map<PeerId, Message>* responses);
  /**
   * Returns false if a response was not Message::kAck or Message::kCantReach.
   * In the latter case, the peer is removed.
   */
  bool undisputableBroadcast(Message* request);

  /**
   * FIXME(tcies) the next two functions will need to go away!!
   */
  std::weak_ptr<Peer> ensure(const std::string& address);
  void getContextAndSocketType(zmq::context_t** context, int* socket_type);

  /**
   * Sends a request to the single specified peer. If the peer is not connected
   * yet, adds permanent connection to the peer.
   */
  void request(const PeerId& peer, Message* request, Message* response);

  static void discoveryHandler(const Message& request, Message* response);

  /**
   * Discovery message type denomination constant
   */
  static const char kDiscovery[];

 private:
  /**
   * Constructor: Performs discovery, fetches metadata and loads into database
   */
  MapApiHub() = default;
  /**
   * 127.0.0.1 if discovery is from file, own LAN address otherwise
   */
  static std::string ownAddressBeforePort();
  /**
   * Thread for listening to peers
   */
  static void listenThread(MapApiHub *self);
  std::thread listener_;
  std::mutex condVarMutex_;
  std::condition_variable listenerStatus_;
  volatile bool listenerConnected_;
  volatile bool terminate_ = false;

  std::unique_ptr<zmq::context_t> context_;
  std::string own_address_;
  /**
   * For now, peers may only be added or accessed, so peer mutex only used for
   * atomic addition of peers.
   */
  std::mutex peer_mutex_;
  typedef std::unordered_map<PeerId, std::unique_ptr<Peer> > PeerMap;
  PeerMap peers_;
  /**
   * Maps message types denominations to handler functions
   */
  static std::unordered_map<std::string,
  std::function<void(const Message& request, Message* response)> >
  handlers_;

  std::unique_ptr<Discovery> discovery_;
};

}

#endif /* MAP_API_HUB_H_ */
