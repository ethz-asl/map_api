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
   * Initialize hub with given IP and port
   */
  bool init(const std::string &ipPort);
  ~MapApiHub();
  /**
   * Re-enter server thread, unbind
   */
  void kill();
  /**
   * Get amount of peers
   */
  int peerSize();
  /**
   * Registers a handler for messages titled with the given name
   * TODO(tcies) create a metatable directory for these types as well
   * The handler must take two arguments: A string which contains the
   * serialized data to be treated and a socket pointer to which a message MUST
   * be sent at the end of the handler
   * TODO(tcies) distinguish between pub/sub and rpc
   */
  bool registerHandler(const std::string& name,
                       std::function<void(const std::string& serialized_type,
                                          proto::HubMessage* socket)> handler);
  /**
   * Sends out the specified message to all connected peers
   */
  void broadcast(const std::string& type, const std::string& serialized);

  static void helloHandler(const std::string& peer, proto::HubMessage* socket);


 private:
  /**
   * Constructor: Performs discovery, fetches metadata and loads into database
   */
  MapApiHub();
  /**
   * Thread for listening to peers
   */
  static void listenThread(MapApiHub *self, const std::string &ipPort);
  std::thread listener_;
  std::mutex condVarMutex_;
  std::condition_variable listenerStatus_;
  volatile bool listenerConnected_;
  volatile bool terminate_;
  /**
   * Context and list of peers
   */
  std::unique_ptr<zmq::context_t> context_;
  Poco::RWLock peerLock_;
  std::set<std::shared_ptr<zmq::socket_t> > peers_;
  /**
   * Handler utilities
   */
  static std::unordered_map<std::string,
  std::function<void(const std::string&, proto::HubMessage*)> >
  handlers_;
};

}

#endif /* MAP_API_HUB_H_ */
