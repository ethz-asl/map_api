#include "map-api/ipc.h"

#include <sstream>
#include <string>

#include <glog/logging.h>

#include "map-api/hub.h"
#include "map-api/logical-time.h"
#include "map-api/peer-id.h"

namespace map_api {

std::mutex IPC::barrier_mutex_, IPC::message_mutex_;
std::condition_variable IPC::barrier_cv_;
std::unordered_map<int, int> IPC::barrier_map_;
std::queue<std::string> IPC::messages_;

const char IPC::kBarrierMessage[] = "map_api_ipc_barrier";
MAP_API_STRING_MESSAGE(IPC::kBarrierMessage);

const char IPC::kMessageMessage[] = "map_api_ipc_message";
MAP_API_STRING_MESSAGE(IPC::kMessageMessage);

IPC::~IPC() {}

void IPC::registerHandlers() {
  Hub::instance().registerHandler(kBarrierMessage, barrierHandler);
  Hub::instance().registerHandler(kMessageMessage, pushHandler);
}

void IPC::barrier(int id, int n_peers) {
  std::ostringstream ss;
  ss << id;
  while (Hub::instance().peerSize() < n_peers) {
    usleep(10000);
  }
  Message barrier_message;
  barrier_message.impose<kBarrierMessage, std::string>(ss.str());
  CHECK(Hub::instance().undisputableBroadcast(&barrier_message));
  std::unique_lock<std::mutex> lock(barrier_mutex_);
  VLOG(3) << "Waiting for " << id;
  while (barrier_map_[id] < n_peers) {
    barrier_cv_.wait(lock);
  }
  barrier_map_[id] = 0;
  VLOG(3) << id << " done";
  lock.unlock();
}

void IPC::barrierHandler(
    const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  std::string id_string;
  request.extract<kBarrierMessage>(&id_string);
  int id = std::stoi(id_string);
  {
    std::lock_guard<std::mutex> lock(barrier_mutex_);
    ++barrier_map_[id];
  }
  barrier_cv_.notify_one();
  VLOG(3) << "Got rpc on " << id << ", map now has " << barrier_map_[id];
  response->ack();
}

template <>
void IPC::push(const std::string& message) {
  Message request;
  request.impose<kMessageMessage>(message);
  CHECK(Hub::instance().undisputableBroadcast(&request));
}
template <>
void IPC::push(const Id& message) {
  push(message.hexString());
}
template <>
void IPC::push(const LogicalTime& message) {
  push(message.serialize());
}
template <>
void IPC::push(const PeerId& peer_id) {
  push(peer_id.ipPort());
}

void IPC::pushHandler(const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  std::string message;
  request.extract<kMessageMessage>(&message);
  std::lock_guard<std::mutex> lock(message_mutex_);
  messages_.push(message);
  response->ack();
}

template <>
bool IPC::pop(std::string* destination) {
  CHECK_NOTNULL(destination);
  std::lock_guard<std::mutex> lock(message_mutex_);
  if (messages_.empty()) {
    LOG(WARNING) << "IPC pop failed";
    return false;
  }
  *destination = messages_.front();
  messages_.pop();
  return true;
}
template <>
bool IPC::pop(Id* destination) {
  CHECK_NOTNULL(destination);
  std::string serialized;
  if (!pop(&serialized)) {
    return false;
  }
  CHECK(destination->fromHexString(serialized));
  return true;
}
template <>
bool IPC::pop(LogicalTime* destination) {
  CHECK_NOTNULL(destination);
  std::string serialized_stream;
  if (!pop(&serialized_stream)) {
    return false;
  }
  std::istringstream iss(serialized_stream);
  uint64_t serialized;
  iss >> serialized;
  *destination = LogicalTime(serialized);
  return true;
}
template <>
bool IPC::pop(PeerId* destination) {
  CHECK_NOTNULL(destination);
  std::string address;
  if (!pop(&address)) {
    return false;
  }
  CHECK(PeerId::isValid(address));
  *destination = PeerId(address);
  return true;
}

}  // namespace map_api
