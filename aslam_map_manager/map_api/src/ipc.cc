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
std::queue<proto::IpcMessage> IPC::messages_;

const char IPC::kBarrierMessage[] = "map_api_ipc_barrier";
MAP_API_STRING_MESSAGE(IPC::kBarrierMessage);

const char IPC::kMessageMessage[] = "map_api_ipc_message";
MAP_API_PROTO_MESSAGE(IPC::kMessageMessage, proto::IpcMessage);

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
void IPC::pushFor(const std::string& message, int receiver) {
  Message request;
  proto::IpcMessage ipc_message;
  ipc_message.set_message(message);
  ipc_message.set_receiver(receiver);
  request.impose<kMessageMessage>(ipc_message);
  CHECK(Hub::instance().undisputableBroadcast(&request));
}
template <>
void IPC::pushFor(const Id& message, int receiver) {
  pushFor(message.hexString(), receiver);
}
template <>
void IPC::pushFor(const LogicalTime& message, int receiver) {
  pushFor(message.serialize(), receiver);
}
template <>
void IPC::pushFor(const PeerId& peer_id, int receiver) {
  pushFor(peer_id.ipPort(), receiver);
}

void IPC::pushHandler(const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  proto::IpcMessage ipc_message;
  request.extract<kMessageMessage>(&ipc_message);
  std::lock_guard<std::mutex> lock(message_mutex_);
  messages_.push(ipc_message);
  response->ack();
}

template <>
bool IPC::popFor(std::string* destination, int receiver) {
  CHECK_NOTNULL(destination);
  std::lock_guard<std::mutex> lock(message_mutex_);
  proto::IpcMessage ipc_message;
  do {
    if (messages_.empty()) {
      LOG(WARNING) << "IPC pop failed";
      return false;
    }
    ipc_message = messages_.front();
    messages_.pop();
  } while (ipc_message.receiver() != receiver);
  *destination = ipc_message.message();
  return true;
}
template <>
bool IPC::popFor(Id* destination, int receiver) {
  CHECK_NOTNULL(destination);
  std::string serialized;
  if (!popFor(&serialized, receiver)) {
    return false;
  }
  CHECK(destination->fromHexString(serialized));
  return true;
}
template <>
bool IPC::popFor(LogicalTime* destination, int receiver) {
  CHECK_NOTNULL(destination);
  std::string serialized_stream;
  if (!popFor(&serialized_stream, receiver)) {
    return false;
  }
  std::istringstream iss(serialized_stream);
  uint64_t serialized;
  iss >> serialized;
  *destination = LogicalTime(serialized);
  return true;
}
template <>
bool IPC::popFor(PeerId* destination, int receiver) {
  CHECK_NOTNULL(destination);
  std::string address;
  if (!popFor(&address, receiver)) {
    return false;
  }
  CHECK(PeerId::isValid(address));
  *destination = PeerId(address);
  return true;
}

}  // namespace map_api
