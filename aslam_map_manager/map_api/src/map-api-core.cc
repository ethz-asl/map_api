#include "map-api/map-api-core.h"

#include <iostream>
#include <cstring>

#include <Poco/Data/Common.h>
#include <Poco/Data/SQLite/Connector.h>
#include <glog/logging.h>
#include <zeromq_cpp/zmq.hpp>

#include "map-api/map-api-hub.h"
#include "map-api/transaction.h"

DEFINE_string(ipPort, "127.0.0.1:5050", "Define node ip and port");

namespace map_api {

MapApiCore &MapApiCore::getInstance() {
  static MapApiCore instance;
  static std::mutex initMutex;
  initMutex.lock();
  if (!instance.isInitialized()) {
    if (!instance.init(FLAGS_ipPort)){
      LOG(FATAL) << "Failed to initialize Map Api Core.";
    }
  }
  initMutex.unlock();
  return instance;
}

MapApiCore::MapApiCore() : owner_(Id::random()),
    hub_(MapApiHub::getInstance()), metatable_(), initialized_(false){}

bool MapApiCore::syncTableDefinition(const proto::TableDescriptor& descriptor) {
  // init metatable if not yet initialized
  if (!metatable_){
    metatable_ = std::unique_ptr<Metatable>(new Metatable(owner_));
    metatable_->init();
  }
  // insert table definition if not exists
  Transaction tryInsert(owner_);
  tryInsert.begin();
  std::shared_ptr<Revision> attempt = metatable_->getTemplate();
  attempt->set("name", descriptor.name());
  attempt->set("descriptor", descriptor);
  tryInsert.insert<CRTableInterface>(*metatable_, attempt);
  tryInsert.addConflictCondition(*metatable_, "name", descriptor.name());
  bool success = tryInsert.commit();
  if (success){
    return true;
  }
  // if has existed, verify descriptors match
  Transaction reader(owner_);
  reader.begin();
  std::shared_ptr<Revision> previous = reader.findUnique(*metatable_, "name",
                                                   descriptor.name());
  CHECK(previous) << "Can't find table " << descriptor.name() <<
      " even though its presence seemingly caused a conflict";
  proto::TableDescriptor previousDescriptor;
  previous->get("descriptor", &previousDescriptor);
  return descriptor.SerializeAsString() ==
      previousDescriptor.SerializeAsString();
}

bool MapApiCore::init(const std::string &ipPort) {
  if (!hub_.init(ipPort)){
    LOG(ERROR) << "Map Api core init failed, could not connect to socket " <<
        ipPort;
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
  dbSess_.reset();
  initialized_ = false;
}

}

