#include "map-api/map-api-hub.h"

#include <iostream>
#include <fstream>
#include <memory>
#include <thread>

#include <glog/logging.h>

#include "map-api/ipc.h"
#include "core.pb.h"

DECLARE_string(ip_port);

namespace map_api {

const char MapApiHub::kDiscovery[] = "map_api_hub_discovery";
MAP_API_MESSAGE_IMPOSE_STRING_MESSAGE(MapApiHub::kDiscovery);

std::unordered_map<std::string,
std::function<void(const std::string&, Message*)> >
MapApiHub::handlers_;

MapApiHub::MapApiHub() : terminate_(false) {}

MapApiHub::~MapApiHub() {
  kill();
}

bool MapApiHub::init(const std::string &ipPort) {
  terminate_ = false;
  // Handlers must be initialized before handler thread is started
  IPC::init(); // TODO(tcies) more apprioprate place for this - gflags style?
  registerHandler(kDiscovery, discoveryHandler);
  // 1. create own server
  context_.reset(new zmq::context_t());
  listenerConnected_ = false;
  listener_ = std::thread(listenThread, this, ipPort);
  {
    std::unique_lock<std::mutex> lock(condVarMutex_);
    listenerStatus_.wait(lock);
  }
  if (!listenerConnected_){
    context_.reset();
    return false;
  }

  // 2. connect to servers already on network (discovery from file)
  std::vector<PeerId> discovery_peers;
  discovery_.getPeers(&discovery_peers);
  peer_mutex_.lock();
  for (const PeerId& peer : discovery_peers) {
    // don't attempt to connect if already connected
    if (peers_.find(peer) != peers_.end()) continue;

    LOG(INFO) << FLAGS_ip_port << ": Found peer " << peer << ", connecting...";
    std::unordered_map<PeerId, std::unique_ptr<Peer> >::iterator inserted =
        peers_.insert(std::make_pair(
            peer, std::unique_ptr<Peer>(new Peer(peer.ipPort(), *context_,
                                                 ZMQ_REQ)))).first;
    // connection request is sent outside the peer_mutex_ lock to avoid
    // deadlocks where two peers try to connect to each other:
    // P1                           P2
    // main thread locks mutex      main thread locks mutex
    // sends out c.req. to P2       sends out c.req. to P1
    // c.hand. tries to lock        c.hand. tries to lock
    // ----------------> DEADLOCK!!! <----------------------
  }
  peer_mutex_.unlock();

  // 3. Report self to discovery
  discovery_.announce();

  // 4. Announce self to peers (who will not revisit discovery)
  Message announce_self;
  announce_self.impose<kDiscovery>(ipPort);
  CHECK(undisputableBroadcast(announce_self));

  return true;
}

MapApiHub &MapApiHub::instance() {
  static MapApiHub instance;
  return instance;
}

void MapApiHub::kill() {
  LOG(INFO) << "Terminate called in " << FLAGS_ip_port;
  if (terminate_){
    LOG(WARNING) << "Double termination";
    return;
  }
  // unbind and re-enter server
  terminate_ = true;
  listener_.join();
  // disconnect from peers (no need to lock as listener should be only other
  // thread)
  for (const std::pair<const PeerId, std::unique_ptr<Peer> >& peer : peers_) {
    peer.second->disconnect();
  }
  peers_.clear();
  // destroy context
  context_.reset();
  discovery_.leave();
}

int MapApiHub::peerSize() {
  int size;
  std::lock_guard<std::mutex> lock(peer_mutex_);
  size = peers_.size();
  return size;
}

bool MapApiHub::registerHandler(
    const char* name,
    std::function<void(const std::string& serialized_type,
                       Message* response)> handler) {
  CHECK_NOTNULL(name);
  CHECK(handler);
  // TODO(tcies) div. error handling
  handlers_[name] = handler;
  return true;
}

void MapApiHub::request(
    const PeerId& peer, const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  std::unordered_map<PeerId, std::unique_ptr<Peer> >::iterator found =
      peers_.find(peer);
  if (found == peers_.end()) {
    std::lock_guard<std::mutex> lock(peer_mutex_);
    // double-checked locking pattern
    std::unordered_map<PeerId, std::unique_ptr<Peer> >::iterator found =
        peers_.find(peer);
    if (found == peers_.end()) {
      found = peers_.insert(std::make_pair(
          peer, std::unique_ptr<Peer>(
              new Peer(peer.ipPort(), *context_, ZMQ_REQ)))).first;
    }
  }
  CHECK(found->second->request(request, response));
}

void MapApiHub::broadcast(const Message& request,
                          std::unordered_map<PeerId, Message>* responses) {
  CHECK_NOTNULL(responses);
  responses->clear();
  // TODO(tcies) parallelize using std::future
  for (const std::pair<const PeerId, std::unique_ptr<Peer> >& peer_pair :
      peers_) {
    if (!peer_pair.second->request(request, &(*responses)[peer_pair.first])) {
      (*responses)[peer_pair.first].impose<Message::kCantReach>();
    }
  }
}

bool MapApiHub::undisputableBroadcast(const Message& request) {
  std::unordered_map<PeerId, Message> responses;
  broadcast(request, &responses);
  for (const std::pair<PeerId, Message>& response : responses) {
    if (!response.second.isType<Message::kAck>()) {
      if (response.second.isType<Message::kCantReach>()) {
        removeUnreachable(response.first);
      }
      else {
        return false;
      }
    }
  }
  return true;
}

void MapApiHub::discoveryHandler(const std::string& peer, Message* response) {
  CHECK_NOTNULL(response);
  LOG(INFO) << "Peer " << peer << " requests discovery, let's "\
      "connect to it...";
  // lock peer set lock so we can write without a race condition
  instance().peer_mutex_.lock();
  if (!instance().peers_.insert(
      std::make_pair(PeerId(peer), std::unique_ptr<Peer>(
          new Peer(peer, *instance().context_, ZMQ_REQ)))).second) {
    LOG(INFO) << peer << " requested discovery, but we have already connected";
  }
  instance().peer_mutex_.unlock();
  // ack by resend
  response->impose<Message::kAck>();
}

void MapApiHub::removeUnreachable(const PeerId& peer) {
  LOG(INFO) << "Removing unreachable peer " << peer;
  std::lock_guard<std::mutex> lock(peer_mutex_);
  std::unordered_map<PeerId, std::unique_ptr<Peer> >::iterator found =
      peers_.find(peer);
  CHECK(found != peers_.end());
  peers_.erase(found);
}

void MapApiHub::listenThread(MapApiHub *self, const std::string &ipPort) {
  zmq::socket_t server(*(self->context_), ZMQ_REP);
  {
    std::unique_lock<std::mutex> lock(self->condVarMutex_);
    // server only lives in this thread
    VLOG(3) << "Bind to " << ipPort;
    try {
      server.bind(("tcp://" + ipPort).c_str());
      self->listenerConnected_ = true;
      lock.unlock();
      self->listenerStatus_.notify_one();
    }
    catch (const std::exception &e) {
      LOG(ERROR) << "Server bind failed with exception \"" << e.what() <<
          "\", ipPort string was " << ipPort;
      self->listenerConnected_ = false;
      lock.unlock();
      self->listenerStatus_.notify_one();
      return;
    }
  }
  int timeOutMs = 100;
  server.setsockopt(ZMQ_RCVTIMEO, &timeOutMs, sizeof(timeOutMs));
  LOG(INFO) << "Server launched on " << ipPort;

  while (true) {
    zmq::message_t request;
    if (!server.recv(&request)) {
      //timeout, check if termination flag?
      if (self->terminate_)
        break;
      else
        continue;
    }
    proto::HubMessage query;
    query.ParseFromArray(request.data(), request.size());

    // Query handler
    std::unordered_map<std::string,
    std::function<void(const std::string&, Message*)> >::iterator handler =
        handlers_.find(query.type());
    CHECK(handlers_.end() != handler) << "Handler for message type " <<
        query.type() << " not registered";
    Message response;
    handler->second(query.serialized(), &response);
    std::string serialized_response = response.SerializeAsString();
    zmq::message_t response_message(serialized_response.size());
    memcpy((void *) response_message.data(), serialized_response.c_str(),
           serialized_response.size());
    server.send(response_message);
  }
  server.close();
  LOG(INFO) << "Listener terminated\n";
}

}
