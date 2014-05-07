/*
 * map-api-hub.h
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#ifndef MAP_API_HUB_H_
#define MAP_API_HUB_H_

#include <cstddef>
#include <string>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <set>

#include <zeromq_cpp/zmq.hpp>
#include <Poco/RWLock.h>

namespace map_api {

/**
 * Map Api Hub: Manages connections to other participating nodes
 */
class MapApiHub {
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
  std::set<std::shared_ptr<zmq::socket_t>> peers_;
};

}

#endif /* MAP_API_HUB_H_ */
