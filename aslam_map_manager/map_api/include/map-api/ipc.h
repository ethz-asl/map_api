#ifndef IPC_H_
#define IPC_H_

#include <mutex>
#include <condition_variable>
#include <unordered_map>

#include <zeromq_cpp/zmq.hpp>

#include "map-api/message.h"
#include "core.pb.h"

namespace map_api {

/**
 * Class containing diverse inter-process communication utilities, tailored
 * to map-api
 */
class IPC {
 public:
  virtual ~IPC();
  /**
   * Registers the handlers at the hub, mostly
   */
  static void init();
  /**
   * Waits for n_peers other peers to call this function with the same id.
   * Any code that comes after the barrier call at either peer gets executed
   * only once all the code before the barrier call has been executed at
   * all peers.
   */
  static void barrier(int id, int n_peers);
  /**
   * Handles barrier calls from other peers
   */
  static void barrierHandler(const std::string& id_string, Message* response);
  /**
   * Barrier message type denomination constant
   */
  static const char kBarrierMessage[];

 private:
  static std::mutex barrier_mutex_;
  static std::condition_variable barrier_cv_;
  static std::unordered_map<int, int> barrier_map_;
};

} /* namespace map_api */

#endif /* IPC_H_ */
