#include "map-api/net-table-manager.h"

#include "map-api/map-api-hub.h"
#include "map-api/map-api-core.h"
#include "map-api/revision.h"

namespace map_api {

REVISION_PROTOBUF(proto::TableDescriptor);

constexpr char kMetaTableName[] = "map_api_metatable";
constexpr char kMetaTableNameField[] = "name";
constexpr char kMetaTableStructureField[] = "structure";
constexpr char kMetaTableChunkHexString[] = "000000000000000000000003E1A1AB7E";

template<>
bool NetTableManager::routeChunkRequestOperations<proto::ChunkRequestMetadata>(
    const proto::ChunkRequestMetadata& request, Message* response,
    TableMap::iterator* found) {
  CHECK_NOTNULL(response);
  CHECK_NOTNULL(found);
  const std::string& table = request.table();
  Id chunk_id;
  CHECK(chunk_id.fromHexString(request.chunk_id()));
  if (!findTable(table, found)) {
    response->impose<Message::kDecline>();
    return false;
  }
  return true;
}

void NetTableManager::registerHandlers() {
  // chunk requests
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

  // chord requests
  MapApiHub::instance().registerHandler(
      NetTableIndex::kRoutedChordRequest, handleRoutedChordRequests);
}

void NetTableManager::init(bool create_metatable_chunk) {
  tables_lock_.writeLock();
  tables_.clear();
  tables_lock_.unlock();
  initMetatable(create_metatable_chunk);
}

void NetTableManager::initMetatable(bool create_metatable_chunk) {
  tables_lock_.writeLock();
  std::pair<TableMap::iterator, bool> inserted =
      tables_.insert(std::make_pair(kMetaTableName,
                                    std::unique_ptr<NetTable>()));
  CHECK(inserted.second);
  inserted.first->second.reset(new NetTable);
  NetTable* metatable = inserted.first->second.get();

  std::unique_ptr<TableDescriptor> metatable_descriptor(new TableDescriptor);
  metatable_descriptor->setName(kMetaTableName);
  metatable_descriptor->addField<std::string>(kMetaTableNameField);
  metatable_descriptor->addField<proto::TableDescriptor>(
      kMetaTableStructureField);
  metatable->init(true, &metatable_descriptor);
  Id metatable_chunk_id;
  CHECK(metatable_chunk_id.fromHexString(kMetaTableChunkHexString));
  tables_lock_.unlock();

  // outside of table lock to avoid deadlock
  if (create_metatable_chunk) {
    metatable_chunk_ = metatable->newChunk(metatable_chunk_id);
  } else {
    // TODO(tcies) spin till successful
    metatable_chunk_ = metatable->getChunk(metatable_chunk_id);
  }
  if (metatable_chunk_ == nullptr) {
    // TODO(tcies) net chunk lookup
    LOG(ERROR) << "Need to implement net chunk lookup!";
  }
  // integrate metatable with other tables for request routing

  if (create_metatable_chunk) {
    metatable->createIndex();
  } else {
    std::set<PeerId> hub_peers;
    MapApiHub::instance().getPeers(&hub_peers);
    PeerId ready_peer;
    // entry point must have ready core
    bool success = false;
    while (!success) {
      for (const PeerId& peer : hub_peers) {
        if (MapApiHub::instance().isReady(peer)) {
          ready_peer = peer;
          success = true;
          break;
        }
      }
    }
    metatable->joinIndex(ready_peer);
  }
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
    std::pair<TableMap::iterator, bool> inserted = tables_.insert(
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

void NetTableManager::kill() {
  tables_lock_.readLock();
  for(const std::pair<const std::string, std::unique_ptr<NetTable> >& table :
      tables_) {
    table.second->kill();
  }
  tables_lock_.unlock();
  tables_lock_.writeLock();
  tables_.clear();
  tables_lock_.unlock();
}

// ========
// HANDLERS
// ========

void NetTableManager::handleConnectRequest(const Message& request,
                                           Message* response) {
  CHECK_NOTNULL(response);
  proto::ChunkRequestMetadata metadata;
  request.extract<Chunk::kConnectRequest>(&metadata);
  const std::string& table = metadata.table();
  Id chunk_id;
  CHECK(chunk_id.fromHexString(metadata.chunk_id()));
  CHECK_NOTNULL(MapApiCore::instance());
  MapApiCore::instance()->tableManager().tables_lock_.readLock();
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = MapApiCore::instance()->tableManager().tables_.find(table);
  if (found == MapApiCore::instance()->tableManager().tables_.end()) {
    MapApiCore::instance()->tableManager().tables_lock_.unlock();
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleConnectRequest(chunk_id, PeerId(request.sender()),
                                      response);
  MapApiCore::instance()->tableManager().tables_lock_.unlock();
}

void NetTableManager::handleInitRequest(
    const Message& request, Message* response) {
  proto::InitRequest init_request;
  request.extract<Chunk::kInitRequest>(&init_request);
  TableMap::iterator found;
  if (routeChunkRequestOperations(init_request, response, &found)) {
    found->second->handleInitRequest(init_request, PeerId(request.sender()),
                                     response);
  }
}

void NetTableManager::handleInsertRequest(
    const Message& request, Message* response) {
  proto::PatchRequest patch_request;
  request.extract<Chunk::kInsertRequest>(&patch_request);
  TableMap::iterator found;
  if (routeChunkRequestOperations(patch_request, response, &found)) {
    Id chunk_id;
    CHECK(chunk_id.fromHexString(patch_request.metadata().chunk_id()));
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
    CHECK(chunk_id.fromHexString(new_peer_request.metadata().chunk_id()));
    PeerId new_peer(new_peer_request.new_peer()), sender(request.sender());
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
    CHECK(chunk_id.fromHexString(patch_request.metadata().chunk_id()));
    Revision to_insert;
    CHECK(to_insert.ParseFromString(patch_request.serialized_revision()));
    PeerId sender(request.sender());
    found->second->handleUpdateRequest(chunk_id, to_insert, sender, response);
  }
}

void NetTableManager::handleRoutedChordRequests(
    const Message& request, Message* response) {
  CHECK_NOTNULL(response);
  proto::RoutedChordRequest routed_request;
  request.extract<NetTableIndex::kRoutedChordRequest>(&routed_request);
  CHECK(routed_request.has_table_name());
  TableMap::iterator table;
  CHECK(findTable(routed_request.table_name(), &table));
  table->second->handleRoutedChordRequests(request, response);
}

bool NetTableManager::findTable(const std::string& table_name,
                                TableMap::iterator* found) {
  CHECK_NOTNULL(found);
  CHECK_NOTNULL(MapApiCore::instance());
  MapApiCore::instance()->tableManager().tables_lock_.readLock();
  *found = MapApiCore::instance()->tableManager().tables_.find(table_name);
  if (*found == MapApiCore::instance()->tableManager().tables_.end()) {
    MapApiCore::instance()->tableManager().tables_lock_.unlock();
    return false;
  }
  MapApiCore::instance()->tableManager().tables_lock_.unlock();
  return true;
}

} /* namespace map_api */
