#include "map-api/map-api-hub.h"

#include <ifaddrs.h>
#include <iostream>
#include <fstream>
#include <memory>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <thread>
#include <unordered_set>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "map-api/file-discovery.h"
#include "map-api/ipc.h"
#include "map-api/logical-time.h"
#include "map-api/map-api-core.h"
#include "map-api/server-discovery.h"
#include "core.pb.h"

const std::string kFileDiscovery = "file";
const std::string kServerDiscovery = "server";
const std::string kLocalhost = "127.0.0.1";
const char kLoopback[] = "lo";

DEFINE_string(discovery_mode, kFileDiscovery,
              ("How new peers are discovered. \"" + kFileDiscovery +
                  "\" or \"" + kServerDiscovery +
                  "\". In the latter case, IP and port of the server must "\
                  "be specified separately with --discovery_server").c_str());
DEFINE_string(discovery_server, "127.0.0.1:5050", "Server to be used for "\
              "server-discovery");

namespace map_api {

const char MapApiHub::kDiscovery[] = "map_api_hub_discovery";
const char MapApiHub::kReady[] = "map_api_hub_ready";

MapApiHub::HandlerMap MapApiHub::handlers_;

bool MapApiHub::init(bool* is_first_peer) {
  CHECK_NOTNULL(is_first_peer);
  context_.reset(new zmq::context_t());
  terminate_ = false;
  if (FLAGS_discovery_mode == kFileDiscovery) {
    discovery_.reset(new FileDiscovery());
  } else if (FLAGS_discovery_mode == kServerDiscovery) {
    discovery_.reset(new ServerDiscovery(FLAGS_discovery_server, *context_));
  } else {
    LOG(FATAL) << "Specified discovery mode unknown";
  }
  // Handlers must be initialized before handler thread is started
  registerHandler(kDiscovery, discoveryHandler);
  registerHandler(kReady, readyHandler);
  // 1. create own server
  listenerConnected_ = false;
  CHECK(peers_.empty());
  listener_ = std::thread(listenThread, this);
  {
    std::unique_lock<std::mutex> lock(condVarMutex_);
    listenerStatus_.wait(lock);
  }
  if (!listenerConnected_){
    context_.reset();
    return false;
  }

  // 2. connect to servers already on network (discovery from file)
  discovery_->lock();
  std::vector<PeerId> discovery_peers;
  discovery_->getPeers(&discovery_peers);
  peer_mutex_.lock();
  for (const PeerId& peer : discovery_peers) {
    // don't attempt to connect if already connected
    if (peers_.find(peer) != peers_.end()) continue;

    PeerMap::iterator inserted = peers_.insert(std::make_pair(
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
  discovery_->announce();

  // 4. Announce self to peers (who will not revisit discovery)
  Message announce_self, response;
  announce_self.impose<kDiscovery>();
  std::unordered_set<PeerId> unreachable;
  for (const std::pair<const PeerId, std::unique_ptr<Peer> >& peer : peers_) {
    if (!peer.second->try_request(&announce_self, &response)) {
      discovery_->remove(peer.first);
      unreachable.insert(peer.first);
    }
  }
  // 5. Remove peers that were not reachable
  if (!unreachable.empty()) {
    std::lock_guard<std::mutex> lock(peer_mutex_);
    for (const PeerId& peer : unreachable) {
      PeerMap::iterator found = peers_.find(peer);
      CHECK(found != peers_.end());
      found->second->disconnect();
      peers_.erase(found);
    }
  }

  *is_first_peer = peers_.empty();

  discovery_->unlock();
  return true;
}

MapApiHub &MapApiHub::instance() {
  static MapApiHub instance;
  return instance;
}

void MapApiHub::kill() {
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
  discovery_->lock();
  discovery_->leave();
  discovery_->unlock();
  discovery_.reset();
  context_.reset();
}

bool MapApiHub::ackRequest(const PeerId& peer, Message* request) {
  CHECK_NOTNULL(request);
  Message response;
  this->request(peer, request, &response);
  return response.isType<Message::kAck>();
}

void MapApiHub::getPeers(std::set<PeerId>* destination) const {
  CHECK_NOTNULL(destination);
  destination->clear();
  for (const std::pair<const PeerId, std::unique_ptr<Peer> >& peer : peers_) {
    destination->insert(peer.first);
  }
}

int MapApiHub::peerSize() {
  int size;
  std::lock_guard<std::mutex> lock(peer_mutex_);
  size = peers_.size();
  return size;
}

const std::string& MapApiHub::ownAddress() const {
  return own_address_;
}

bool MapApiHub::registerHandler(
    const char* name,
    std::function<void(const Message& serialized_type,
                       Message* response)> handler) {
  CHECK_NOTNULL(name);
  CHECK(handler);
  // TODO(tcies) div. error handling
  handlers_[name] = handler;
  return true;
}

void MapApiHub::request(
    const PeerId& peer, Message* request, Message* response) {
  CHECK_NOTNULL(request);
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
  found->second->request(request, response);
}

bool MapApiHub::try_request(
    const PeerId& peer, Message* request, Message* response) {
  CHECK_NOTNULL(request);
  CHECK_NOTNULL(response);
  PeerMap::iterator found = peers_.find(peer);
  if (found == peers_.end()) {
    LOG(INFO) << "couldn't find " << peer << " among " << peers_.size();
    for (const PeerMap::value_type& peer : peers_) {
      LOG(INFO) << peer.first;
    }
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
  return found->second->try_request(request, response);
}

void MapApiHub::broadcast(Message* request,
                          std::unordered_map<PeerId, Message>* responses) {
  CHECK_NOTNULL(request);
  CHECK_NOTNULL(responses);
  responses->clear();
  // TODO(tcies) parallelize using std::future
  for (const std::pair<const PeerId, std::unique_ptr<Peer> >& peer_pair :
      peers_) {
    peer_pair.second->request(request, &(*responses)[peer_pair.first]);
  }
}

bool MapApiHub::undisputableBroadcast(Message* request) {
  CHECK_NOTNULL(request);
  std::unordered_map<PeerId, Message> responses;
  broadcast(request, &responses);
  for (const std::pair<PeerId, Message>& response : responses) {
    if (!response.second.isType<Message::kAck>()) {
      return false;
    }
  }
  return true;
}

bool MapApiHub::isReady(const PeerId& peer) {
  Message ready_request, response;
  ready_request.impose<kReady>();
  request(peer, &ready_request, &response);
  return response.isType<Message::kAck>();
}

void MapApiHub::discoveryHandler(const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  // lock peer set lock so we can write without a race condition
  instance().peer_mutex_.lock();
  instance().peers_.insert(
      std::make_pair(PeerId(request.sender()), std::unique_ptr<Peer>(
          new Peer(request.sender(), *instance().context_, ZMQ_REQ))));
  instance().peer_mutex_.unlock();
  // ack by resend
  response->impose<Message::kAck>();
}

void MapApiHub::readyHandler(const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  CHECK(request.isType<kReady>());
  if (MapApiCore::instance() == nullptr) {
    response->decline();
  } else {
    response->ack();
  }
}

std::string MapApiHub::ownAddressBeforePort() {
  if (FLAGS_discovery_mode == kFileDiscovery) {
    return kLocalhost;
  } else if (FLAGS_discovery_mode == kServerDiscovery) {
    struct ifaddrs* interface_addresses;
    CHECK(getifaddrs(&interface_addresses) != -1);
    char host[NI_MAXHOST];
    bool success = false;
    for (struct ifaddrs* interface_address = interface_addresses;
        interface_address != NULL;
        interface_address = interface_address->ifa_next) {
      if (interface_address->ifa_addr != NULL) {
        // ignore non-ip4 interfaces
        if (interface_address->ifa_addr->sa_family == AF_INET) {
          // ignore local loopback
          if (strcmp(interface_address->ifa_name, kLoopback) != 0){
            // assuming that first address that satisfies these conditions
            // is the right one TODO(tcies) some day, ability to specify
            // custom interface name might be nice
            CHECK(getnameinfo(interface_address->ifa_addr,
                              sizeof(struct sockaddr_in), host, NI_MAXHOST,
                              NULL,0, NI_NUMERICHOST) == 0);
            success = true;
            break;
          }
        }
      }
    }
    CHECK(success) << "Couldn't determine own LAN address!";
    return std::string(host);
  } else {
    LOG(FATAL) << "Specified discovery mode unknown";
    return "";
  }
}

void MapApiHub::listenThread(MapApiHub *self) {
  const unsigned int kMinPort = 1024;
  const unsigned int kMaxPort = 65536;
  zmq::socket_t server(*(self->context_), ZMQ_REP);
  {
    std::unique_lock<std::mutex> lock(self->condVarMutex_);

    std::mt19937_64 rng(
        std::chrono::high_resolution_clock::now().time_since_epoch().count());
    while (true) {
      unsigned int port = kMinPort + (rng() % (kMaxPort - kMinPort));
      try {
        std::ostringstream address;
        address << ownAddressBeforePort() << ":" << port;
        server.bind(("tcp://" + address.str()).c_str());
        self->own_address_ = address.str();
        break;
      }
      catch (const std::exception &e) {
        port = kMinPort + (rng() % (kMaxPort - kMinPort));
      }
    }
    self->listenerConnected_ = true;
    lock.unlock();
    self->listenerStatus_.notify_one();
  }
  int timeOutMs = 100;
  server.setsockopt(ZMQ_RCVTIMEO, &timeOutMs, sizeof(timeOutMs));

  while (true) {
    zmq::message_t request;
    if (!server.recv(&request)) {
      //timeout, check if termination flag?
      if (self->terminate_)
        break;
      else
        continue;
    }
    Message query;
    CHECK(query.ParseFromArray(request.data(), request.size()));
    LogicalTime::synchronize(LogicalTime(query.logical_time()));

    // Query handler
    HandlerMap::iterator handler = handlers_.find(query.type());
    if (handler == handlers_.end()) {
      for (const HandlerMap::value_type& handler : handlers_) {
        LOG(INFO) << handler.first;
      }
      LOG(FATAL) << "Handler for message type " << query.type() <<
          " not registered";
    }
    Message response;
    //    LOG(INFO) << PeerId::self() << " received request " << query.type();
    handler->second(query, &response);
    //    LOG(INFO) << PeerId::self() << " handled request " << query.type();
    response.set_sender(PeerId::self().ipPort());
    response.set_logical_time(LogicalTime::sample().serialize());
    std::string serialized_response = response.SerializeAsString();
    zmq::message_t response_message(serialized_response.size());
    memcpy((void *) response_message.data(), serialized_response.c_str(),
           serialized_response.size());
    server.send(response_message);
  }
  server.close();
}

}
