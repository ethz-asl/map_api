#include "map-api/net-table-manager.h"

#include "map-api/chunk-transaction.h"
#include "map-api/core.h"
#include "map-api/hub.h"
#include "map-api/revision.h"
#include "./net-table.pb.h"

namespace map_api {

REVISION_PROTOBUF(proto::TableDescriptor);
REVISION_PROTOBUF(proto::PeerList);

constexpr char kMetaTableNameField[] = "name";
constexpr char kMetaTableStructureField[] = "structure";
constexpr char kMetaTableUpdateableField[] = "updateable";
constexpr char kMetaTableParticipantsField[] = "participants";
constexpr char kMetaTableChunkHexString[] = "000000000000000000000003E1A1AB7E";

const char NetTableManager::kMetaTableName[] = "map_api_metatable";

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
  Hub::instance().registerHandler(Chunk::kConnectRequest, handleConnectRequest);
  Hub::instance().registerHandler(Chunk::kInitRequest, handleInitRequest);
  Hub::instance().registerHandler(Chunk::kInsertRequest, handleInsertRequest);
  Hub::instance().registerHandler(Chunk::kLeaveRequest, handleLeaveRequest);
  Hub::instance().registerHandler(Chunk::kLockRequest, handleLockRequest);
  Hub::instance().registerHandler(Chunk::kNewPeerRequest, handleNewPeerRequest);
  Hub::instance().registerHandler(Chunk::kUnlockRequest, handleUnlockRequest);
  Hub::instance().registerHandler(Chunk::kUpdateRequest, handleUpdateRequest);

  // chord requests
  Hub::instance().registerHandler(NetTableIndex::kRoutedChordRequest,
                                  handleRoutedChordRequests);
}

NetTableManager& NetTableManager::instance() {
  static NetTableManager instance;
  return instance;
}

void NetTableManager::init(bool create_metatable_chunk) {
  tables_lock_.writeLock();
  tables_.clear();
  tables_lock_.unlock();
  initMetatable(create_metatable_chunk);
}

void NetTableManager::initMetatable(bool create_metatable_chunk) {
  tables_lock_.writeLock();
  // 1. ALLOCATION
  // the metatable is created in the tables_ structure in order to allow RPC
  // forwarding in the same way as for other tables
  std::pair<TableMap::iterator, bool> inserted =
      tables_.insert(std::make_pair(kMetaTableName,
                                    std::unique_ptr<NetTable>()));
  CHECK(inserted.second);
  inserted.first->second.reset(new NetTable);
  NetTable* metatable = inserted.first->second.get();
  // 2. INITIALIZATION OF STRUCTURE
  std::unique_ptr<TableDescriptor> metatable_descriptor(new TableDescriptor);
  metatable_descriptor->setName(kMetaTableName);
  metatable_descriptor->addField<std::string>(kMetaTableNameField);
  metatable_descriptor->addField<proto::TableDescriptor>(
      kMetaTableStructureField);
  metatable_descriptor->addField<bool>(kMetaTableUpdateableField);
  metatable_descriptor->addField<proto::PeerList>(kMetaTableParticipantsField);
  metatable->init(CRTable::Type::CRU, &metatable_descriptor);
  tables_lock_.unlock();
  // 3. INITIALIZATION OF INDEX
  // outside of table lock to avoid deadlock
  if (create_metatable_chunk) {
    metatable->createIndex();
  } else {
    std::set<PeerId> hub_peers;
    Hub::instance().getPeers(&hub_peers);
    PeerId ready_peer;
    // choosing a ready entry point avoids issues of parallelism such as that
    // e.g. the other peer is at 2. but not at 3. of this procedure.
    bool success = false;
    while (!success) {
      for (const PeerId& peer : hub_peers) {
        if (Hub::instance().isReady(peer)) {
          ready_peer = peer;
          success = true;
          break;
        }
      }
    }
    metatable->joinIndex(ready_peer);
  }
  // 4. CREATE OR FETCH METATABLE CHUNK
  Id metatable_chunk_id;
  CHECK(metatable_chunk_id.fromHexString(kMetaTableChunkHexString));
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
}

void NetTableManager::addTable(
    CRTable::Type type, std::unique_ptr<TableDescriptor>* descriptor) {
  CHECK_NOTNULL(descriptor);
  CHECK(*descriptor);
  TableDescriptor* descriptor_raw = descriptor->get();  // needed later
  tables_lock_.writeLock();
  TableMap::iterator found = tables_.find((*descriptor)->name());
  if (found != tables_.end()) {
    LOG(WARNING) << "Table already defined! Checking consistency...";
    std::unique_ptr<NetTable> temp(new NetTable);
    temp->init(type, descriptor);
    std::shared_ptr<Revision> left = found->second->getTemplate(),
        right = temp->getTemplate();
    CHECK(left->structureMatch(*right));
  } else {
    std::pair<TableMap::iterator, bool> inserted = tables_.insert(
        std::make_pair((*descriptor)->name(), std::unique_ptr<NetTable>()));
    CHECK(inserted.second) << tables_.size();
    inserted.first->second.reset(new NetTable);
    CHECK(inserted.first->second->init(type, descriptor));
  }
  tables_lock_.unlock();
  bool first;
  PeerId entry_point;
  // Ensure validity of table structure
  CHECK(syncTableDefinition(type, *descriptor_raw, &first, &entry_point));
  // May receive requests at this point TODO(tcies) defer them
  NetTable& table = getTable(descriptor_raw->name());
  if (first) {
    table.createIndex();
  } else {
    table.joinIndex(entry_point);
  }
}

NetTable& NetTableManager::getTable(const std::string& name) {
  tables_lock_.readLock();
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = tables_.find(name);
  // TODO(tcies) load table schema from metatable if not active
  CHECK(found != tables_.end()) << "Table not found: " << name;
  tables_lock_.unlock();
  return *found->second;
}

void NetTableManager::tableList(std::vector<std::string>* tables) {
  CHECK_NOTNULL(tables);
  tables->clear();
  tables_lock_.readLock();
  for (const std::pair<const std::string, std::unique_ptr<NetTable> >& pair :
       tables_) {
    tables->push_back(pair.first);
  }
  tables_lock_.unlock();
}

void NetTableManager::kill() {
  tables_lock_.readLock();
  for (const std::pair<const std::string, std::unique_ptr<NetTable> >& table :
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
  CHECK_NOTNULL(Core::instance());
  instance().tables_lock_.readLock();
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = instance().tables_.find(table);
  if (found == instance().tables_.end()) {
    instance().tables_lock_.unlock();
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleConnectRequest(chunk_id, PeerId(request.sender()),
                                      response);
  instance().tables_lock_.unlock();
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

bool NetTableManager::syncTableDefinition(
    CRTable::Type type, const TableDescriptor& descriptor, bool* first,
    PeerId* entry_point) {
  CHECK_NOTNULL(first);
  CHECK_NOTNULL(entry_point);
  CHECK_NOTNULL(metatable_chunk_);
  ChunkTransaction try_insert(metatable_chunk_);
  NetTable& metatable = getTable(kMetaTableName);
  std::shared_ptr<Revision> attempt = metatable.getTemplate();
  Id metatable_id;
  map_api::generateId(&metatable_id);
  attempt->set(CRTable::kIdField, metatable_id);
  attempt->set(kMetaTableNameField, descriptor.name());
  proto::PeerList peers;
  peers.add_peers(PeerId::self().ipPort());
  attempt->set(kMetaTableParticipantsField, peers);
  attempt->set(kMetaTableStructureField, descriptor);
  switch (type) {
    case CRTable::Type::CR:
      attempt->set(kMetaTableUpdateableField, false);
      break;
    case CRTable::Type::CRU:
      attempt->set(kMetaTableUpdateableField, true);
      break;
    default:
      LOG(FATAL) << "Unknown table type";
  }
  try_insert.insert(attempt);
  try_insert.addConflictCondition(kMetaTableNameField, descriptor.name());

  if (try_insert.commit()) {
    *first = true;
    return true;
  } else {
    *first = false;
  }

  // Case Table definition already in metatable
  while (true) {
    ChunkTransaction try_join(metatable_chunk_);
    // 1. Read previous registration in metatable
    std::shared_ptr<Revision> previous =
        try_join.findUnique(kMetaTableNameField, descriptor.name());
    CHECK(previous) << "Can't find table " << descriptor.name() <<
        " even though its presence seemingly caused a conflict";
    // 2. Verify structure
    TableDescriptor previous_descriptor;
    previous->get(kMetaTableStructureField, &previous_descriptor);
    CHECK_EQ(descriptor.SerializeAsString(),
             previous_descriptor.SerializeAsString());
    bool previous_updateable;
    previous->get(kMetaTableUpdateableField, &previous_updateable);
    switch (type) {
      case CRTable::Type::CR:
        CHECK(!previous_updateable);
        break;
      case CRTable::Type::CRU:
        CHECK(previous_updateable);
        break;
      default:
        LOG(FATAL) << "Unknown table type";
    }
    // 3. Pick entry point peer
    proto::PeerList peers;
    previous->get(kMetaTableParticipantsField, &peers);
    CHECK_EQ(1, peers.peers_size()) << "Current implementation assumes only "\
        "one entry point peer per table";
    *entry_point = PeerId(peers.peers(0));
    // 4. Register as peer
    // TODO(tcies) later, this isn't necessary for the demo
    break;
  }
  return true;
}

bool NetTableManager::findTable(const std::string& table_name,
                                TableMap::iterator* found) {
  CHECK_NOTNULL(found);
  instance().tables_lock_.readLock();
  *found = instance().tables_.find(table_name);
  if (*found == instance().tables_.end()) {
    instance().tables_lock_.unlock();
    return false;
  }
  instance().tables_lock_.unlock();
  return true;
}

} /* namespace map_api */
