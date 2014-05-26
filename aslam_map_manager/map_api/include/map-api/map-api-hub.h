#ifndef MAP_API_HUB_H_
#define MAP_API_HUB_H_

#include <cstddef>
#include <string>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <set>
#include <unordered_map>

#include <zeromq_cpp/zmq.hpp>
#include <Poco/RWLock.h>

namespace map_api {

/**
 * Map Api Hub: Manages connections to other participating nodes
 */
class MapApiHub final {
 public:
  /**
   * Get singleton instance of Map Api Hub
   */
  static MapApiHub& getInstance();
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
   */
  bool registerHandler(const std::string& name,
                       void (*handler)(const std::string& serialized_type,
                           zmq::socket_t* socket));

  static void helloHandler(const std::string& peer, zmq::socket_t* socket);

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
  void(*)(const std::string&, zmq::socket_t*)>
  handlers_;
};

}

#endif /* MAP_API_HUB_H_ */
