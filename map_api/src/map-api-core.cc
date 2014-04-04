#include "map-api/map-api-core.h"

#include <iostream>
#include <cstring>

#include <zmq.hpp>
#include <Poco/Data/Common.h>
#include <Poco/Data/SQLite/Connector.h>
#include <glog/logging.h>

#include "map-api/map-api-hub.h"

namespace map_api {

MapApiCore &MapApiCore::getInstance() {
  static MapApiCore instance;
  return instance;
}

MapApiCore::MapApiCore() : hub_(MapApiHub::getInstance()),
    initialized_(false){}

bool MapApiCore::init(const std::string &ipPort) {
  if (!hub_.init(ipPort)){
    return false;
  }
  // TODO(titus) SigAbrt handler?
  Poco::Data::SQLite::Connector::registerConnector();
  dbSess_ = std::shared_ptr<Poco::Data::Session>(
      new Poco::Data::Session("SQLite", "database.db"));
  LOG(INFO)<< "Connected to database..." << std::endl;

  // TODO(tcies) metatable

  initialized_ = true;
  return true;
}

std::shared_ptr<Poco::Data::Session> MapApiCore::getSession(){
  return dbSess_;
}

bool MapApiCore::isInitialized() const{
  return initialized_;
}

void MapApiCore::kill(){
  hub_.kill();
  dbSess_ =  std::shared_ptr<Poco::Data::Session>();
  initialized_ = false;
}

}

