#include "map-api/ipc.h"

#include <sstream>
#include <string>

#include <glog/logging.h>

#include "map-api/map-api-hub.h"

namespace map_api {

std::mutex IPC::barrier_mutex_;
std::condition_variable IPC::barrier_cv_;
std::unordered_map<int, int> IPC::barrier_map_;

IPC::~IPC() {}

void IPC::init() {
  MapApiHub::instance().registerHandler("barrier", barrierHandler);
}

void IPC::barrier(int id, int n_peers) {
  std::ostringstream ss;
  ss << id;
  // TODO(tcies) smarter, cv on peer increase instead of spinning
  while (MapApiHub::instance().peerSize() < n_peers) {
    usleep(10);
  }
  MapApiHub::instance().broadcast("barrier", ss.str());
  std::unique_lock<std::mutex> lock(barrier_mutex_);
  while (barrier_map_[id] < n_peers) {
    barrier_cv_.wait(lock);
  }
  barrier_map_[id] = 0;
  lock.unlock();
}

void IPC::barrierHandler(
    const std::string& id_string, proto::HubMessage* response) {
  int id = std::stoi(id_string);
  {
    std::lock_guard<std::mutex> lock(barrier_mutex_);
    ++barrier_map_[id];
  }
  barrier_cv_.notify_one();
  response->set_name(""); // FIXME(tcies) centralize declarations
  response->set_serialized("");
}

} /* namespace map_api */
