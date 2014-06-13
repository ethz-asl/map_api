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

const std::string MapApiCore::kMetatableNameField = "name";
const std::string MapApiCore::kMetatableDescriptorField = "descriptor";

REVISION_PROTOBUF(TableDescriptor);

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
    hub_(MapApiHub::instance()), initialized_(false){}

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
bool MapApiCore::init(const std::string &ipPort) {
  if (!hub_.init(ipPort)){
    LOG(ERROR) << "Map Api core init failed, could not connect to socket " <<
        ipPort;
    return false;
  }
  Poco::Data::SQLite::Connector::registerConnector();
  dbSess_ = std::make_shared<Poco::Data::Session>("SQLite", ":memory:");
  // ready metatable
  table_manager_.init();
  metatable_.reset(new CRTableRAMCache);
  initialized_ = true;
  return true;
}

std::weak_ptr<Poco::Data::Session> MapApiCore::getSession() {
  return dbSess_;
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
  hub_.kill();
  dbSess_.reset();
  initialized_ = false;
}

NetTableManager& MapApiCore::tableManager() {
  return table_manager_;
}

void MapApiCore::resetDb() {
  CHECK_EQ(1, dbSess_.use_count());
  // don't forget to wipe the disk database if using one
  dbSess_.reset(new Poco::Data::Session("SQLite", ":memory:"));
}

}

