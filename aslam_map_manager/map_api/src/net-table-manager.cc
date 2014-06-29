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
    std::unique_ptr<NetTable> temp(new NetTable);
    temp->init(updateable, descriptor);
    std::shared_ptr<Revision> left = found->second->getTemplate(),
        right = temp->getTemplate();
    CHECK(left->structureMatch(*right));
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

void NetTableManager::handleConnectRequest(const Message& request,
                                           Message* response) {
  CHECK_NOTNULL(response);
  proto::ConnectRequest connect_request;
  request.extract<Chunk::kConnectRequest>(&connect_request);
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
    const Message& request, Message* response) {
  proto::InitRequest init_request;
  request.extract<Chunk::kInitRequest>(&init_request);
  TableMap::iterator found;
  if (routeChunkRequestOperations(init_request, response, &found)) {
    found->second->handleInitRequest(init_request, response);
  }
}

void NetTableManager::handleInsertRequest(
    const Message& request, Message* response) {
  proto::PatchRequest patch_request;
  request.extract<Chunk::kInsertRequest>(&patch_request);
  TableMap::iterator found;
  if (routeChunkRequestOperations(patch_request, response, &found)) {
    Id chunk_id;
    CHECK(chunk_id.fromHexString(patch_request.chunk_id()));
    Revision to_insert;
    CHECK(to_insert.ParseFromString(patch_request.serialized_revision()));
    found->second->handleInsertRequest(chunk_id, to_insert, response);
  }
}

void NetTableManager::handleLeaveRequest(
    const Message& request, Message* response) {
  TableMap::iterator found;
  Id chunk_id;
  PeerId peer;
  if (routeChunkMetadataRequestOperations<Chunk::kLeaveRequest>(
      request, response, &found, &chunk_id, &peer)) {
    found->second->handleLeaveRequest(chunk_id, peer, response);
  }
}

void NetTableManager::handleLockRequest(
    const Message& request, Message* response) {
  TableMap::iterator found;
  Id chunk_id;
  PeerId peer;
  if (routeChunkMetadataRequestOperations<Chunk::kLockRequest>(
      request, response, &found, &chunk_id, &peer)) {
    found->second->handleLockRequest(chunk_id, peer, response);
  }
}

void NetTableManager::handleNewPeerRequest(
    const Message& request, Message* response) {
  proto::NewPeerRequest new_peer_request;
  request.extract<Chunk::kNewPeerRequest>(&new_peer_request);
  TableMap::iterator found;
  if (routeChunkRequestOperations(new_peer_request, response, &found)) {
    Id chunk_id;
    CHECK(chunk_id.fromHexString(new_peer_request.chunk_id()));
    PeerId new_peer(new_peer_request.new_peer()),
        sender(new_peer_request.from_peer());
    found->second->handleNewPeerRequest(chunk_id, new_peer, sender, response);
  }
}

void NetTableManager::handleUnlockRequest(
    const Message& request, Message* response) {
  TableMap::iterator found;
  Id chunk_id;
  PeerId peer;
  if (routeChunkMetadataRequestOperations<Chunk::kUnlockRequest>(
      request, response, &found, &chunk_id, &peer)) {
    found->second->handleUnlockRequest(chunk_id, peer, response);
  }
}

void NetTableManager::handleUpdateRequest(
    const Message& request, Message* response) {
  proto::PatchRequest patch_request;
  request.extract<Chunk::kUpdateRequest>(&patch_request);
  TableMap::iterator found;
  if (routeChunkRequestOperations(patch_request, response, &found)) {
    Id chunk_id;
    CHECK(chunk_id.fromHexString(patch_request.chunk_id()));
    Revision to_insert;
    CHECK(to_insert.ParseFromString(patch_request.serialized_revision()));
    PeerId sender(patch_request.from_peer());
    found->second->handleUpdateRequest(chunk_id, to_insert, sender, response);
  }
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
