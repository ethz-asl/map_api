#include "map-api/ipc.h"

#include <sstream>
#include <string>

#include <glog/logging.h>

#include "map-api/map-api-hub.h"
#include "map-api/peer-id.h"

namespace map_api {

std::mutex IPC::barrier_mutex_, IPC::message_mutex_;
std::condition_variable IPC::barrier_cv_;
std::unordered_map<int, int> IPC::barrier_map_;
std::queue<std::string> IPC::messages_;

const char IPC::kBarrierMessage[] = "map_api_ipc_barrier";
MAP_API_MESSAGE_IMPOSE_STRING_MESSAGE(IPC::kBarrierMessage);

const char IPC::kMessageMessage[] = "map_api_ipc_message";
MAP_API_MESSAGE_IMPOSE_STRING_MESSAGE(IPC::kMessageMessage);

IPC::~IPC() {}

void IPC::init() {
  MapApiHub::instance().registerHandler(kBarrierMessage, barrierHandler);
  MapApiHub::instance().registerHandler(kMessageMessage, pushHandler);
}

void IPC::barrier(int id, int n_peers) {
  std::ostringstream ss;
  ss << id;
  while (MapApiHub::instance().peerSize() < n_peers) {
    usleep(10000);
  }
  Message barrier_message;
  barrier_message.impose<kBarrierMessage,std::string>(ss.str());
  CHECK(MapApiHub::instance().undisputableBroadcast(barrier_message));
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
  response->ack();
}

void IPC::push(const std::string& message) {
  Message request;
  request.impose<kMessageMessage>(message);
  CHECK(MapApiHub::instance().undisputableBroadcast(request));
}

void IPC::pushHandler(const std::string& message, Message* response) {
  std::lock_guard<std::mutex> lock(message_mutex_);
  messages_.push(message);
  response->ack();
}

bool IPC::pop(std::string* destination) {
  CHECK_NOTNULL(destination);
  std::lock_guard<std::mutex> lock(message_mutex_);
  if (messages_.empty()) {
    return false;
  }
  *destination = messages_.front();
  messages_.pop();
  return true;
}

} /* namespace map_api */
