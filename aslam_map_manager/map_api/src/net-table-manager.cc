#include "map-api/net-table-manager.h"

#include "map-api/map-api-hub.h"
#include "map-api/map-api-core.h"

namespace map_api {

void NetTableManager::init() {
  MapApiHub::instance().registerHandler(
      kConnectRequest, handleConnectRequest);
  MapApiHub::instance().registerHandler(
      kInsertRequest, handleInsertRequest);
  MapApiHub::instance().registerHandler(
      Chunk::kLeaveRequest, handleLeaveRequest);
  MapApiHub::instance().registerHandler(
      Chunk::kLockRequest, handleLockRequest);
  MapApiHub::instance().registerHandler(
      Chunk::kNewPeerRequest, handleNewPeerRequest);
  MapApiHub::instance().registerHandler(
      kParticipationRequest, handleParticipationRequest);
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
  tables_.clear();
}

// ======================
// MESSAGE SPECIFICATIONS
// ======================

const char NetTableManager::kConnectRequest[] = "map_api_chunk_connect";
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(NetTableManager::kConnectRequest,
                                     proto::ConnectRequest);
const char NetTableManager::kConnectResponse[] =
    "map_api_chunk_connect_response";
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(NetTableManager::kConnectResponse,
                                     proto::ConnectResponse);
const char NetTableManager::kParticipationRequest[] =
    "map_api_chunk_participation";
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(NetTableManager::kParticipationRequest,
                                     proto::ParticipationRequest);
const char NetTableManager::kInsertRequest[] = "map_api_chunk_insert";
MAP_API_MESSAGE_IMPOSE_PROTO_MESSAGE(NetTableManager::kInsertRequest,
                                     proto::InsertRequest);
const char NetTableManager::kChunkNotOwned[] = "map_api_chunk_not_owned";


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

void NetTableManager::handleInsertRequest(
    const std::string& serialized_request, Message* response) {
  CHECK_NOTNULL(response);
  CHECK(false);
  /** TODO(tcies) re-enable with TableManager
  // parse message TODO(tcies) centralize process?
  proto::InsertRequest insert_request;
  CHECK(insert_request.ParseFromString(serialized_request));
  // determine addressed chunk
  Id requested_chunk;
  CHECK(requested_chunk.fromHexString(insert_request.chunk_id()));
  ChunkMap::iterator chunk_iterator =
      instance().active_chunks_.find(requested_chunk);
  if (chunk_iterator == instance().active_chunks_.end()) {
    response->impose<kChunkNotOwned>();
    return;
  }
  std::shared_ptr<Chunk> addressedChunk = chunk_iterator->second;
  // insert revision into chunk
  Revision to_insert;
  to_insert.ParseFromString(insert_request.serialized_revision());
  CHECK(addressedChunk->handleInsert(to_insert));
   */
  response->impose<Message::kAck>();
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
    found->second->handleLeaveRequest(chunk_id, peer, response);
  }
}
void NetTableManager::handleNewPeerRequest(
    const std::string& serialized_request, Message* response) {
  proto::NewPeerRequest request;
  CHECK(request.ParseFromString(serialized_request));
  TableMap::iterator found;
  if (routeChunkRequestOperations(request, response, &found)) {
    Id chunk_id;
    chunk_id.fromHexString(request.chunk_id());
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
    found->second->handleLeaveRequest(chunk_id, peer, response);
  }
}

void NetTableManager::handleParticipationRequest(
    const std::string& serialized_request, Message* response) {
  CHECK_NOTNULL(response);
  proto::ParticipationRequest request;
  CHECK(request.ParseFromString(serialized_request));
  LOG(INFO) << "Received participation request for table " << request.table()
                              << " from peer " << request.from_peer();
  Id chunk_id;
  CHECK(chunk_id.fromHexString(request.chunk_id()));
  if (MapApiCore::instance().tableManager().getTable(request.table()).
      has(chunk_id)) {
    response->impose<Message::kRedundant>();
    return;
  }
  // what if requested table is not loaded?
  // TODO(tcies) Load table schema from metatable
  MapApiCore::instance().tableManager().getTable(request.table()).connectTo(
      chunk_id, PeerId(request.from_peer()));
  response->impose<Message::kAck>();
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
