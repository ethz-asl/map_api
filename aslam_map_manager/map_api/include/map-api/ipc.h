#ifndef MAP_API_IPC_H_
#define MAP_API_IPC_H_

#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>

#include "map-api/message.h"
#include "map-api/unique-id.h"
#include "./core.pb.h"
#include "./ipc.pb.h"

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
  static void registerHandlers();
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
  static void barrierHandler(const Message& request, Message* response);
  /**
   * Allows to broadcast an object to all other peers
   */
  template <typename Type>
  static void push(const Type& message);
  template <typename Type>
  static void pushFor(const Type& message, int receiver);
  static void pushHandler(const Message& request, Message* response);
  /**
   * Read the oldest broadcast message (false if empty queue). pop() skips all
   * messages sent to specific peers while popFor() skips all messages sent to
   * all peers.
   */
  template <typename Type>
  static bool pop(Type* destination);
  template <typename Type>
  static bool popFor(Type* destination, int receiver);

  /**
   * Message declarations
   */
  static const char kBarrierMessage[];
  static const char kMessageMessage[];

 private:
  static std::mutex barrier_mutex_;
  static std::condition_variable barrier_cv_;
  static std::unordered_map<int, int> barrier_map_;

  static std::mutex message_mutex_;
  static std::queue<proto::IpcMessage> messages_;

  static constexpr int kEveryone = -1;
};

}  // namespace map_api

#include "map-api/ipc-inl.h"

#endif  // MAP_API_IPC_H_
