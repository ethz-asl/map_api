#ifndef DMAP_HUB_H_
#define DMAP_HUB_H_

#include <functional>
#include <string>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <set>
#include <unordered_map>

#include <zeromq_cpp/zmq.hpp>

#include <map-api/discovery.h>
#include <map-api/peer.h>
#include <map-api/peer-id.h>

namespace map_api {
class Message;

/**
 * Map API Hub: Manages connections to other participating nodes
 */
class Hub final {
 public:
  /**
   * Get singleton instance of Map API Hub
   */
  static Hub& instance();
  /**
   * Initialize hub
   */
  bool init(bool* is_first_peer);
  /**
   * Re-enter server thread, disconnect from peers, leave discovery
   */
  void kill();
  /**
   * Same as request(), but expects Message::kAck as response and returns false
   * if something else is received
   */
  bool ackRequest(const PeerId& peer, Message* request);

  // Overload for ackRequest where request has a unique message type name.
  // Requires specification of Message::UniqueType. This specialization is
  // included in the DMAP_UNIQUE_PROTO_MESSAGE macro in message.h.
  template <typename RequestType>
  bool ackRequest(const PeerId& peer, const RequestType& request);

  /**
   * Lists the addresses of connected peers, ordered set for user convenience
   */
  void getPeers(std::set<PeerId>* destination) const;

  bool hasPeer(const PeerId& peer) const;
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
                       const std::function<void(const Message& request,
                                                Message* response)>& handler);
  // Register handler for request and response types defined with
  // DMAP_UNIQUE_PROTO_MESSAGE from message.h.
  // Using C-style function pointers since this makes the typical use case more
  // user friendly; this allows passing static functions without the need to
  // wrap in std::function.
  template <typename RequestType, typename ResponseType>
  bool registerHandler(
      void (*handler)(const RequestType& request, ResponseType* response));
  // Same for handlers that return a bool that corresponds to ack/decline.
  template <typename RequestType>
  bool registerHandler(bool (*handler)(const RequestType& request));

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

  // Overload for request and response type have a unique message type name
  // and where a specific type of response is expected. Requires specification
  // of Message::UniqueType. This specialization is included in the
  // DMAP_UNIQUE_PROTO_MESSAGE macro in message.h.
  template <typename RequestType, typename ResponseType>
  void request(
      const PeerId& peer, const RequestType& request, ResponseType* response);

  /**
   * Returns false if timeout
   */
  bool try_request(const PeerId& peer, Message* request, Message* response);
  /**
   * Returns true if peer is ready, i.e. has an initialized core
   */
  bool isReady(const PeerId& peer);

  static void discoveryHandler(const Message& request, Message* response);
  static void readyHandler(const Message& request, Message* response);

  /**
   * Default RPCs
   */
  static const char kDiscovery[];
  static const char kReady[];

 private:
  /**
   * Constructor: Performs discovery, fetches metadata and loads into database
   */
  Hub() = default;
  /**
   * 127.0.0.1 if discovery is from file, own LAN address otherwise
   */
  static std::string ownAddressBeforePort();
  /**
   * Thread for listening to peers
   */
  static void listenThread(Hub* self);

  void logIncoming(const size_t size, const std::string& type);
  void logOutgoing(const size_t size, const std::string& type);
  friend class Peer;

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
  typedef std::unordered_map<
      std::string, std::function<void(const Message&, Message*)> > HandlerMap;
  HandlerMap handlers_;

  std::unique_ptr<Discovery> discovery_;

  std::unique_ptr<internal::NetworkDataLog> data_log_in_, data_log_out_;
  std::mutex m_in_log_, m_out_log_;

  static const std::string kInDataLogPrefix, kOutDataLogPrefix;
};

}  // namespace map_api

#include "./hub-inl.h"

#endif  // DMAP_HUB_H_
