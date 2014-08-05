#include "map-api/map-api-core.h"

#include <iostream>
#include <cstring>

#include <Poco/Data/Common.h>
#include <Poco/Data/SQLite/Connector.h>
#include <glog/logging.h>
#include <zeromq_cpp/zmq.hpp>

#include "map-api/ipc.h"
#include "map-api/map-api-hub.h"
#include "map-api/local-transaction.h"

namespace map_api {

MapApiCore MapApiCore::instance_;

const std::string MapApiCore::kMetatableNameField = "name";
const std::string MapApiCore::kMetatableDescriptorField = "descriptor";

std::shared_ptr<Poco::Data::Session> MapApiCore::db_session_;
bool MapApiCore::db_session_initialized_ = false;

REVISION_PROTOBUF(TableDescriptor);

MapApiCore* MapApiCore::instance() {
  if (!instance_.initialized_mutex_.try_lock()) {
    return nullptr;
  } else {
    if (instance_.initialized_) {
      instance_.initialized_mutex_.unlock();
      return &instance_;
    } else {
      instance_.initialized_mutex_.unlock();
      return nullptr;
    }
  }
}

void MapApiCore::initializeInstance() {
  std::unique_lock<std::mutex> lock(instance_.initialized_mutex_);
  CHECK(!instance_.initialized_);
  instance_.init();
  lock.unlock();
  CHECK_NOTNULL(instance());
}

MapApiCore::MapApiCore() : hub_(MapApiHub::instance()),
    table_manager_(NetTableManager::instance()), initialized_(false){}

// can't initialize metatable in init, as its initialization calls
// MapApiCore::getInstance, which again calls this
void MapApiCore::init() {
  IPC::registerHandlers();
  NetTableManager::registerHandlers();
  bool is_first_peer;
  if (!hub_.init(&is_first_peer)){
    LOG(FATAL) << "Map Api core init failed";
  }
  Poco::Data::SQLite::Connector::registerConnector();
  db_session_ = std::make_shared<Poco::Data::Session>("SQLite", ":memory:");
  db_session_initialized_ = true;
  // ready metatable
  table_manager_.init(is_first_peer);
  initialized_ = true;
}

std::weak_ptr<Poco::Data::Session> MapApiCore::getSession() {
  CHECK(db_session_initialized_);
  return db_session_;
}

bool MapApiCore::isInitialized() const {
  return initialized_;
}

void MapApiCore::kill() {
  table_manager_.kill();
  hub_.kill();
  db_session_.reset();
  initialized_ = false; // TODO(tcies) re-order?
}

MapApiCore::~MapApiCore() {
  CHECK(initialized_mutex_.try_lock());
  if (initialized_) {
    kill(); // TODO(tcies) could fail - require of user to invoke instead?
  }
}

}

