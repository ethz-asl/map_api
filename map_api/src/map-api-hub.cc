/*
 * map-api-hub.cc
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#include "map-api/map-api-hub.h"

#include <iostream>
#include <fstream>
#include <memory>
#include <thread>

#include <zmq.hpp>
#include <glog/logging.h>

#include "core.pb.h"

#define FAKE_DISCOVERY "/tmp/mapapi-discovery.txt"

namespace map_api {

MapApiHub::MapApiHub() : terminate_(false) {

}

MapApiHub::~MapApiHub(){
  std::ofstream cleanDiscovery(FAKE_DISCOVERY, std::ios::out);
  cleanDiscovery << std::endl;
  cleanDiscovery.close();
  terminate_ = true;
  listener_.join();
}

bool MapApiHub::init(const std::string &ipPort){
  // FOR NOW: FAKE DISCOVERY

  // 1. create own server
  context_ = std::unique_ptr<zmq::context_t>(new zmq::context_t());
  listener_ = std::thread(listenThread, this, ipPort);

  // 2. connect to servers already on network (discovery from file)
  std::ifstream discovery(FAKE_DISCOVERY, std::ios::in);
  bool is_already_registered = false;
  peerLock_.writeLock();
  for (std::string other; getline(discovery,other);){
    if (other.compare("") == 0) continue;
    if (other.compare(ipPort) == 0){
      LOG(INFO) << "Found registration of self from previous dead run, will "\
          "not register...";
      is_already_registered = true;
      continue;
    }
    LOG(INFO) << "Found peer " << other << ", connecting...";
    auto it = peers_.insert(std::unique_ptr<zmq::socket_t>(
        new zmq::socket_t(*context_, ZMQ_REQ))).first;
    (*it)->connect(("tcp://" + other).c_str());
  }
  peerLock_.unlock();
  discovery.close();

  // 3. put own socket into discovery file
  if (!is_already_registered){
    std::ofstream report(FAKE_DISCOVERY, std::ios::out | std::ios::app);
    report << ipPort << std::endl;
    report.close();
  }

  // 4. notify peers of self
  peerLock_.readLock();
  for (auto peer : peers_){
    proto::NodeQueryUnion query;
    query.set_type(proto::NodeQueryUnion_Type_HELLO);
    query.mutable_hello()->set_from(ipPort);
    std::string queryString = query.SerializeAsString();
    zmq::message_t message((void*)queryString.c_str(),queryString.size(),NULL,
                           NULL);
    peer->send(message);
    LOG(INFO) << "Peer notified of self" << std::endl;
    peer->recv(&message);
  }
  peerLock_.unlock();

  return true;
}

MapApiHub &MapApiHub::getInstance(){
  static MapApiHub instance;
  return instance;
}

void MapApiHub::listenThread(MapApiHub *self, const std::string &ipPort){
  zmq::socket_t server(*(self->context_), ZMQ_REP);
  // server only lives in this thread
  server.bind(("tcp://"+ipPort).c_str());
  int to = 1000;
  server.setsockopt(ZMQ_RCVTIMEO,&to,sizeof(to));
  LOG(INFO) << "Server launched..." << std::endl;

  while (true){
    zmq::message_t message;
    if (!server.recv(&message)){
      //timeout, check if termination flag?
      if (self->terminate_)
        break;
      else
        continue;
    }
    proto::NodeQueryUnion query;
    query.ParseFromArray(message.data(), message.size());

    // Query handler
    // TODO(tcies): Move handler elsewhere?
    // TODO(tcies): Use http://code.google.com/p/rpcz/ ?
    switch(query.type()){

      // A new node says Hello: Connect to its publisher
      case proto::NodeQueryUnion_Type_HELLO:{
        LOG(INFO) << "Peer " << query.hello().from() << " says hello, let's "\
            "connect to it..." << std::endl;
        // lock peer set lock so we can write without a race condition
        self->peerLock_.writeLock();
        auto it = self->peers_.insert(std::unique_ptr<zmq::socket_t>(
            new zmq::socket_t(*(self->context_), ZMQ_SUB))).first;
        (*it)->connect(("tcp://" + query.hello().from()).c_str());
        // ack by resend
        server.send(message);
        break;
      }

      // Not recognized:
      default:{
        LOG(ERROR) << "Message " << query.type() << " not recognized"
        << std::endl;
      }
    }
  }
  LOG(INFO) << "Listener terminated\n";
}
}
