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
}

void NetTableManager::addTable(std::unique_ptr<TableDescriptor>* descriptor) {
  std::pair<std::unordered_map<std::string, std::unique_ptr<NetCRTable> >::
  iterator, bool> inserted = tables_.insert(
      std::make_pair((*descriptor)->name(), std::unique_ptr<NetCRTable>()));
  CHECK(inserted.second);
  inserted.first->second.reset(new NetCRTable);
  CHECK(inserted.first->second->init(descriptor));
}

NetCRTable& NetTableManager::getTable(const std::string& name) {
  std::unordered_map<std::string, std::unique_ptr<NetCRTable> >::iterator
  found = tables_.find(name);
  // TODO(tcies) load table schema from metatable if not active
  CHECK(found != tables_.end());
  return *found->second;
}
const NetCRTable& NetTableManager::getTable(const std::string& name) const {
  std::unordered_map<std::string, std::unique_ptr<NetCRTable> >::const_iterator
  found = tables_.find(name);
  CHECK(found != tables_.end());
  return *found->second;
}

void NetTableManager::clear() {
  for(const std::pair<const std::string, std::unique_ptr<NetCRTable> >& table :
      tables_) {
    table.second->leaveAllChunks();
  }
  tables_.clear();
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
  std::unordered_map<std::string, std::unique_ptr<NetCRTable> >::iterator
  found = MapApiCore::instance().tableManager().tables_.find(table);
  if (found == MapApiCore::instance().tableManager().tables_.end()) {
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleConnectRequest(chunk_id, from_peer, response);
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
  *found = MapApiCore::instance().tableManager().tables_.find(table_name);
  if (*found == MapApiCore::instance().tableManager().tables_.end()) {
    return false;
  }
  return true;
}

} /* namespace map_api */
