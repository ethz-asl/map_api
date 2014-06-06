#include "map-api/ipc.h"

#include <sstream>
#include <string>

#include <glog/logging.h>

#include "map-api/map-api-hub.h"

namespace map_api {

std::mutex IPC::barrier_mutex_;
std::condition_variable IPC::barrier_cv_;
std::unordered_map<int, int> IPC::barrier_map_;

const char IPC::kBarrierMessage[] = "map_api_ipc_barrier";
MAP_API_MESSAGE_IMPOSE_STRING_MESSAGE(IPC::kBarrierMessage);

IPC::~IPC() {}

void IPC::init() {
  MapApiHub::instance().registerHandler(kBarrierMessage, barrierHandler);
}

void IPC::barrier(int id, int n_peers) {
  std::ostringstream ss;
  ss << id;
  // TODO(tcies) smarter, cv on peer increase instead of spinning
  while (MapApiHub::instance().peerSize() < n_peers) {
    usleep(10);
  }
  Message barrier_message;
  barrier_message.impose<kBarrierMessage,std::string>(ss.str());
  MapApiHub::instance().broadcast(barrier_message);
  std::unique_lock<std::mutex> lock(barrier_mutex_);
  while (barrier_map_[id] < n_peers) {
    barrier_cv_.wait(lock);
  }
  barrier_map_[id] = 0;
  lock.unlock();
}

void IPC::barrierHandler(
    const std::string& id_string, Message* response) {
  CHECK_NOTNULL(response);
  int id = std::stoi(id_string);
  {
    std::lock_guard<std::mutex> lock(barrier_mutex_);
    ++barrier_map_[id];
  }
  barrier_cv_.notify_one();
  response->impose<Message::kAck>();
}

} /* namespace map_api */
