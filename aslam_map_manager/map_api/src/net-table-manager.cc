#include "map-api/net-table-manager.h"

#include "map-api/map-api-hub.h"
#include "map-api/map-api-core.h"

namespace map_api {

void NetTableManager::init() {
  MapApiHub::instance().registerHandler(
      Chunk::kConnectRequest, handleConnectRequest);
  MapApiHub::instance().registerHandler(
      Chunk::kInitRequest, handleInitRequest);
  MapApiHub::instance().registerHandler(
      Chunk::kInsertRequest, handleInsertRequest);
  MapApiHub::instance().registerHandler(
      Chunk::kLeaveRequest, handleLeaveRequest);
  MapApiHub::instance().registerHandler(
      Chunk::kLockRequest, handleLockRequest);
  MapApiHub::instance().registerHandler(
      Chunk::kNewPeerRequest, handleNewPeerRequest);
  MapApiHub::instance().registerHandler(
      Chunk::kUnlockRequest, handleUnlockRequest);
  MapApiHub::instance().registerHandler(
      Chunk::kUpdateRequest, handleUpdateRequest);
  tables_lock_.writeLock();
  tables_.clear();
  tables_lock_.unlock();
}

void NetTableManager::addTable(
    bool updateable, std::unique_ptr<TableDescriptor>* descriptor) {
  CHECK_NOTNULL(descriptor);
  CHECK(*descriptor);
  tables_lock_.readLock();
  TableMap::iterator found = tables_.find((*descriptor)->name());
  if (found != tables_.end()) {
    LOG(INFO) << "Table already active";
    NetTable* temp = new NetTable;
    temp->init(updateable, descriptor);
    std::shared_ptr<Revision> left = found->second->getTemplate(),
        right = temp->getTemplate();
    CHECK(left->structureMatch(*right));
    delete temp;
  } else {
    std::pair<std::unordered_map<std::string, std::unique_ptr<NetTable> >::
    iterator, bool> inserted = tables_.insert(
        std::make_pair((*descriptor)->name(), std::unique_ptr<NetTable>()));
    CHECK(inserted.second) << tables_.size();
    inserted.first->second.reset(new NetTable);
    CHECK(inserted.first->second->init(updateable, descriptor));
  }
  tables_lock_.unlock();
}

NetTable& NetTableManager::getTable(const std::string& name) {
  tables_lock_.readLock();
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = tables_.find(name);
  // TODO(tcies) load table schema from metatable if not active
  CHECK(found != tables_.end());
  tables_lock_.unlock();
  return *found->second;
}

void NetTableManager::leaveAllChunks() {
  tables_lock_.readLock();
  for(const std::pair<const std::string, std::unique_ptr<NetTable> >& table :
      tables_) {
    table.second->leaveAllChunks();
  }
  tables_lock_.unlock();
}

// ========
// HANDLERS
// ========

void NetTableManager::handleConnectRequest(const std::string& serialized_request,
                                           Message* response) {
  CHECK_NOTNULL(response);
  proto::ConnectRequest connect_request;
  CHECK(connect_request.ParseFromString(serialized_request));
  const std::string& table = connect_request.table();
  Id chunk_id;
  CHECK(chunk_id.fromHexString(connect_request.chunk_id()));
  PeerId from_peer(connect_request.from_peer());
  MapApiCore::instance().tableManager().tables_lock_.readLock();
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = MapApiCore::instance().tableManager().tables_.find(table);
  if (found == MapApiCore::instance().tableManager().tables_.end()) {
    MapApiCore::instance().tableManager().tables_lock_.unlock();
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleConnectRequest(chunk_id, from_peer, response);
  MapApiCore::instance().tableManager().tables_lock_.unlock();
}

void NetTableManager::handleInitRequest(
    const std::string& serialized_request, Message* response) {
  proto::InitRequest request;
  CHECK(request.ParseFromString(serialized_request));
  TableMap::iterator found;
  if (routeChunkRequestOperations(request, response, &found)) {
    found->second->handleInitRequest(request, response);
  }
}

void NetTableManager::handleInsertRequest(
    const std::string& serialized_request, Message* response) {
  proto::PatchRequest request;
  CHECK(request.ParseFromString(serialized_request));
  TableMap::iterator found;
  if (routeChunkRequestOperations(request, response, &found)) {
    Id chunk_id;
    CHECK(chunk_id.fromHexString(request.chunk_id()));
    Revision to_insert;
    CHECK(to_insert.ParseFromString(request.serialized_revision()));
    found->second->handleInsertRequest(chunk_id, to_insert, response);
  }
}

void NetTableManager::handleLeaveRequest(
    const std::string& serialized_request, Message* response) {
  TableMap::iterator found;
  Id chunk_id;
  PeerId peer;
  if (routeChunkMetadataRequestOperations(
      serialized_request, response, &found, &chunk_id, &peer)) {
    found->second->handleLeaveRequest(chunk_id, peer, response);
  }
}

void NetTableManager::handleLockRequest(
    const std::string& serialized_request, Message* response) {
  TableMap::iterator found;
  Id chunk_id;
  PeerId peer;
  if (routeChunkMetadataRequestOperations(
      serialized_request, response, &found, &chunk_id, &peer)) {
    found->second->handleLockRequest(chunk_id, peer, response);
  }
}

void NetTableManager::handleNewPeerRequest(
    const std::string& serialized_request, Message* response) {
  proto::NewPeerRequest request;
  CHECK(request.ParseFromString(serialized_request));
  TableMap::iterator found;
  if (routeChunkRequestOperations(request, response, &found)) {
    Id chunk_id;
    CHECK(chunk_id.fromHexString(request.chunk_id()));
    PeerId new_peer(request.new_peer()), sender(request.from_peer());
    found->second->handleNewPeerRequest(chunk_id, new_peer, sender, response);
  }
}

void NetTableManager::handleUnlockRequest(
    const std::string& serialized_request, Message* response) {
  TableMap::iterator found;
  Id chunk_id;
  PeerId peer;
  if (routeChunkMetadataRequestOperations(
      serialized_request, response, &found, &chunk_id, &peer)) {
    found->second->handleUnlockRequest(chunk_id, peer, response);
  }
}

void NetTableManager::handleUpdateRequest(
    const std::string& serialized_request, Message* response) {
  proto::PatchRequest request;
  CHECK(request.ParseFromString(serialized_request));
  TableMap::iterator found;
  if (routeChunkRequestOperations(request, response, &found)) {
    Id chunk_id;
    CHECK(chunk_id.fromHexString(request.chunk_id()));
    Revision to_insert;
    CHECK(to_insert.ParseFromString(request.serialized_revision()));
    PeerId sender(request.from_peer());
    found->second->handleUpdateRequest(chunk_id, to_insert, sender, response);
  }
}

bool NetTableManager::routeChunkMetadataRequestOperations(
    const std::string& serialized_request, Message* response,
    TableMap::iterator* found, Id* chunk_id, PeerId* peer) {
  CHECK_NOTNULL(chunk_id);
  CHECK_NOTNULL(peer);
  proto::ChunkRequestMetadata metadata;
  CHECK(metadata.ParseFromString(serialized_request));
  chunk_id->fromHexString(metadata.chunk_id());
  *peer = PeerId(metadata.from_peer());
  return routeChunkRequestOperations(metadata, response, found);
}

bool NetTableManager::findTable(const std::string& table_name,
                                TableMap::iterator* found) {
  CHECK_NOTNULL(found);
  MapApiCore::instance().tableManager().tables_lock_.readLock();
  *found = MapApiCore::instance().tableManager().tables_.find(table_name);
  if (*found == MapApiCore::instance().tableManager().tables_.end()) {
    MapApiCore::instance().tableManager().tables_lock_.unlock();
    return false;
  }
  MapApiCore::instance().tableManager().tables_lock_.unlock();
  return true;
}

} /* namespace map_api */
