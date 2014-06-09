#include "map-api/map-api-core.h"

#include <iostream>
#include <cstring>

#include <Poco/Data/Common.h>
#include <Poco/Data/SQLite/Connector.h>
#include <glog/logging.h>
#include <zeromq_cpp/zmq.hpp>

#include "map-api/map-api-hub.h"
#include "map-api/local-transaction.h"

DEFINE_string(ip_port, "127.0.0.1:5050", "Define node ip and port");

namespace map_api {

MapApiCore &MapApiCore::instance() {
  static MapApiCore instance;
  static std::mutex initMutex;
  initMutex.lock();
  if (!instance.isInitialized()) {
    if (!instance.init(FLAGS_ip_port)){
      LOG(FATAL) << "Failed to initialize Map Api Core.";
    }
  }
  initMutex.unlock();
  return instance;
}

MapApiCore::MapApiCore() : owner_(Id::random()),
    hub_(MapApiHub::instance()), chunk_manager_(ChunkManager::instance()),
    initialized_(false){}

bool MapApiCore::syncTableDefinition(const proto::TableDescriptor& descriptor) {
  // init metatable if not yet initialized TODO(tcies) better solution?
  ensureMetatable();
  // insert table definition if not exists
  LocalTransaction tryInsert;
  tryInsert.begin();
  std::shared_ptr<Revision> attempt = Metatable::instance().getTemplate();
  attempt->set(Metatable::kNameField, descriptor.name());
  attempt->set(Metatable::kDescriptorField, descriptor);
  tryInsert.insert(Metatable::instance(), attempt);
  tryInsert.addConflictCondition(Metatable::instance(), Metatable::kNameField,
                                 descriptor.name());
  bool success = tryInsert.commit();
  if (success){
    return true;
  }
  // if has existed, verify descriptors match
  LocalTransaction reader;
  reader.begin();
  std::shared_ptr<Revision> previous = reader.findUnique(
      Metatable::instance(), Metatable::kNameField, descriptor.name());
  CHECK(previous) << "Can't find table " << descriptor.name() <<
      " even though its presence seemingly caused a conflict";
  proto::TableDescriptor previousDescriptor;
  previous->get(Metatable::kDescriptorField, &previousDescriptor);
  if (descriptor.SerializeAsString() !=
      previousDescriptor.SerializeAsString()) {
    LOG(ERROR) << "Table schema mismatch of table " << descriptor.name() << ": "
        << "Desired structure is " << descriptor.DebugString() <<
        " while structure in metatable is " << previousDescriptor.DebugString();
    return false;
  }
  return true;
}

// can't initialize metatable in init, as its initialization calls
// MapApiCore::getInstance, which again calls this
bool MapApiCore::init(const std::string &ipPort) {
  if (!hub_.init(ipPort)){
    LOG(ERROR) << "Map Api core init failed, could not connect to socket " <<
        ipPort;
    return false;
  }
  // chunk_manager_.init(); TODO(tcies) reactivate once TableManager ready
  // TODO(titus) SigAbrt handler?
  Poco::Data::SQLite::Connector::registerConnector();
  dbSess_ = std::make_shared<Poco::Data::Session>("SQLite", ":memory:");
  initialized_ = true;
  return true;
}

std::weak_ptr<Poco::Data::Session> MapApiCore::getSession() {
  return dbSess_;
}

inline void MapApiCore::ensureMetatable() {
  // TODO(tcies) put this in Metatable::instance, resp. create an
  // initializedMeyersInstance() function
  if (!Metatable::instance().isInitialized()) {
    CHECK(Metatable::instance().init());
  }
}

bool MapApiCore::isInitialized() const {
  return initialized_;
}

void MapApiCore::kill() {
  hub_.kill();
  dbSess_.reset();
  initialized_ = false;
}

void MapApiCore::resetDb() {
  CHECK_EQ(1, dbSess_.use_count());
  dbSess_.reset(new Poco::Data::Session("SQLite", ":memory:"));
  Metatable::instance().init();
}

}

