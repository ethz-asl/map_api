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
  std::lock_guard<std::mutex> lock(instance_.initialized_mutex_);
  CHECK(!instance_.initialized_);
  instance_.init();
}

MapApiCore::MapApiCore() : hub_(MapApiHub::instance()), initialized_(false){}

bool MapApiCore::syncTableDefinition(const TableDescriptor& descriptor) {
  // init metatable if not yet initialized TODO(tcies) better solution?
  ensureMetatable();
  // insert table definition if not exists
  LocalTransaction tryInsert;
  tryInsert.begin();
  std::shared_ptr<Revision> attempt = metatable_->getTemplate();
  attempt->set(kMetatableNameField, descriptor.name());
  attempt->set(kMetatableDescriptorField, descriptor);
  tryInsert.insert(attempt, metatable_.get());
  tryInsert.addConflictCondition(kMetatableNameField, descriptor.name(),
                                 metatable_.get());
  bool success = tryInsert.commit();
  if (success){
    return true;
  }
  // if has existed, verify descriptors match
  LocalTransaction reader;
  reader.begin();
  std::shared_ptr<Revision> previous = reader.findUnique(
      kMetatableNameField, descriptor.name(), metatable_.get());
  CHECK(previous) << "Can't find table " << descriptor.name() <<
      " even though its presence seemingly caused a conflict";
  TableDescriptor previousDescriptor;
  previous->get(kMetatableDescriptorField, &previousDescriptor);
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
  metatable_.reset(new CRTableRAMCache);
  initialized_ = true;
}

std::weak_ptr<Poco::Data::Session> MapApiCore::getSession() {
  CHECK(db_session_initialized_);
  return db_session_;
}

void MapApiCore::initMetatable() {
  std::unique_ptr<TableDescriptor> metatable_descriptor(new TableDescriptor);
  metatable_descriptor->setName("metatable");
  metatable_descriptor->addField<std::string>(kMetatableNameField);
  metatable_descriptor->addField<TableDescriptor>(kMetatableDescriptorField);
  CHECK(metatable_->init(&metatable_descriptor));
}

void MapApiCore::ensureMetatable() {
  if (!metatable_->isInitialized()){
    initMetatable();
  }
}

bool MapApiCore::isInitialized() const {
  return initialized_;
}

void MapApiCore::kill() {
  table_manager_.kill();
  hub_.kill();
  db_session_.reset();
  initialized_ = false;
}

MapApiCore::~MapApiCore() {
  kill(); // TODO(tcies) could fail - require of user to invoke instead?
}

NetTableManager& MapApiCore::tableManager() {
  return table_manager_;
}
const NetTableManager& MapApiCore::tableManager() const {
  return table_manager_;
}

}

