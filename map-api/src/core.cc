#include <map-api/core.h>
#include <iostream>  // NOLINT
#include <cstring>

#include <Poco/Data/Common.h>
#include <Poco/Data/SQLite/Connector.h>
#include <glog/logging.h>
#include <zeromq_cpp/zmq.hpp>

#include <map-api/hub.h>
#include <map-api/ipc.h>

namespace map_api {

Core Core::instance_;

const std::string Core::kMetatableNameField = "name";
const std::string Core::kMetatableDescriptorField = "descriptor";

std::shared_ptr<Poco::Data::Session> Core::db_session_;
bool Core::db_session_initialized_ = false;

MAP_API_REVISION_PROTOBUF(TableDescriptor);

Core* Core::instance() {
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

void Core::initializeInstance() {
  std::unique_lock<std::mutex> lock(instance_.initialized_mutex_);
  CHECK(!instance_.initialized_);
  instance_.init();
  lock.unlock();
  CHECK_NOTNULL(instance());
}

Core::Core()
    : hub_(Hub::instance()),
      table_manager_(NetTableManager::instance()),
      initialized_(false) {}

// can't initialize metatable in init, as its initialization calls
// MapApiCore::getInstance, which again calls this
void Core::init() {
  IPC::registerHandlers();
  NetTableManager::registerHandlers();
  bool is_first_peer;
  if (!hub_.init(&is_first_peer)) {
    LOG(FATAL) << "Map Api core init failed";
  }
  Poco::Data::SQLite::Connector::registerConnector();
  db_session_ = std::make_shared<Poco::Data::Session>("SQLite", ":memory:");
  db_session_initialized_ = true;
  // ready metatable
  table_manager_.init(is_first_peer);
  initialized_ = true;
}

std::weak_ptr<Poco::Data::Session> Core::getSession() {
  CHECK(db_session_initialized_);
  return db_session_;
}

bool Core::isInitialized() const { return initialized_; }

void Core::kill() {
  table_manager_.kill();
  hub_.kill();
  db_session_.reset();
  initialized_ = false;  // TODO(tcies) re-order?
}

Core::~Core() {
  CHECK(initialized_mutex_.try_lock());
  if (initialized_) {
    kill();  // TODO(tcies) could fail - require of user to invoke instead?
  }
}

}  // namespace map_api
