#include "map-api/net-table-manager.h"

#include "map-api/chunk-transaction.h"
#include "map-api/core.h"
#include "map-api/hub.h"
#include "map-api/legacy-chunk.h"
#include "map-api/raft-chunk.h"
#include "map-api/raft-node.h"
#include "map-api/revision.h"
#include "./net-table.pb.h"

namespace map_api {

MAP_API_REVISION_PROTOBUF(proto::TableDescriptor);
MAP_API_REVISION_PROTOBUF(proto::PeerList);

enum MetaTableFields {
  kMetaTableNameField,
  kMetaTableStructureField,
  kMetaTableParticipantsField,
  kMetaTableListenersField
};

constexpr char kMetaTableChunkHexString[] = "000000000000000000000003E1A1AB7E";

const char NetTableManager::kMetaTableName[] = "map_api_metatable";

NetTableManager::NetTableManager()
    : metatable_chunk_(nullptr), metatable_(nullptr) {}

template <>
bool NetTableManager::getTableForRequestWithStringOrDecline<std::string>(
    const std::string& request, Message* response, TableMap::iterator* found) {
  CHECK_NOTNULL(response);
  CHECK_NOTNULL(found);
  if (!findTable(request, found)) {
    response->impose<Message::kDecline>();
    return false;
  }
  return true;
}

template <>
bool NetTableManager::getTableForRequestWithMetadataOrDecline<
    proto::ChunkRequestMetadata>(const proto::ChunkRequestMetadata& request,
                                 Message* response, TableMap::iterator* found) {
  return getTableForRequestWithStringOrDecline(request.table(), response,
                                               found);
}

void NetTableManager::registerHandlers() {
  // LegacyChunk requests.
  Hub::instance().registerHandler(LegacyChunk::kConnectRequest,
                                  handleConnectRequest);
  Hub::instance().registerHandler(LegacyChunk::kInitRequest, handleInitRequest);
  Hub::instance().registerHandler(LegacyChunk::kInsertRequest,
                                  handleInsertRequest);
  Hub::instance().registerHandler(LegacyChunk::kLeaveRequest,
                                  handleLeaveRequest);
  Hub::instance().registerHandler(LegacyChunk::kLockRequest, handleLockRequest);
  Hub::instance().registerHandler(LegacyChunk::kNewPeerRequest,
                                  handleNewPeerRequest);
  Hub::instance().registerHandler(LegacyChunk::kUnlockRequest,
                                  handleUnlockRequest);
  Hub::instance().registerHandler(LegacyChunk::kUpdateRequest,
                                  handleUpdateRequest);

  // RaftChunk requests.
  Hub::instance().registerHandler(RaftNode::kConnectRequest,
                                  handleRaftConnectRequest);
  Hub::instance().registerHandler(RaftNode::kInitRequest,
                                  handleRaftInitRequest);
  Hub::instance().registerHandler(RaftNode::kAppendEntries,
                                  handleRaftAppendRequest);
  Hub::instance().registerHandler(RaftNode::kInsertRequest, handleRaftInsertRequest);
  Hub::instance().registerHandler(RaftNode::kUpdateRequest,
                                  handleRaftUpdateRequest);
  Hub::instance().registerHandler(RaftNode::kVoteRequest,
                                  handleRaftRequestVote);
  Hub::instance().registerHandler(RaftNode::kJoinQuitRequest,
                                  handleRaftJoinQuitRequest);
  Hub::instance().registerHandler(RaftNode::kNotifyJoinQuitSuccess,
                                  handleRaftNotifyJoinQuitSuccess);
  Hub::instance().registerHandler(RaftNode::kQueryState, handleRaftQueryState);

  // Net table requests.
  Hub::instance().registerHandler(NetTable::kPushNewChunksRequest,
                                  handlePushNewChunksRequest);
  Hub::instance().registerHandler(NetTable::kAnnounceToListeners,
                                  handleAnnounceToListenersRequest);
  Hub::instance().registerHandler(SpatialIndex::kTriggerRequest,
                                  handleSpatialTriggerNotification);

  // Chord requests.
  Hub::instance().registerHandler(NetTableIndex::kRoutedChordRequest,
                                  handleRoutedNetTableChordRequests);
  // Spatial index requests.
  Hub::instance().registerHandler(SpatialIndex::kRoutedChordRequest,
                                  handleRoutedSpatialChordRequests);
}

NetTableManager& NetTableManager::instance() {
  static NetTableManager instance;
  return instance;
}

void NetTableManager::init(bool create_metatable_chunk) {
  tables_lock_.acquireWriteLock();
  tables_.clear();
  tables_lock_.releaseWriteLock();
  initMetatable(create_metatable_chunk);
}

void NetTableManager::initMetatable(bool create_metatable_chunk) {
  tables_lock_.acquireWriteLock();
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
  std::shared_ptr<TableDescriptor> metatable_descriptor(new TableDescriptor);
  metatable_descriptor->setName(kMetaTableName);
  metatable_descriptor->addField<std::string>(kMetaTableNameField);
  metatable_descriptor->addField<proto::TableDescriptor>(
      kMetaTableStructureField);
  metatable_descriptor->addField<proto::PeerList>(kMetaTableParticipantsField);
  metatable_descriptor->addField<proto::PeerList>(kMetaTableListenersField);
  metatable->init(metatable_descriptor);
  tables_lock_.releaseWriteLock();
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
  common::Id metatable_chunk_id;
  CHECK(metatable_chunk_id.fromHexString(kMetaTableChunkHexString));
  if (create_metatable_chunk) {
    metatable_chunk_ = metatable->newChunk(metatable_chunk_id);
  } else {
    // TODO(tcies) spin till successful
    metatable_chunk_ = metatable->getChunk(metatable_chunk_id);
  }
}

NetTable* NetTableManager::addTable(
    std::shared_ptr<TableDescriptor> descriptor) {
  CHECK(descriptor);
  TableDescriptor* descriptor_raw = descriptor.get();  // needed later

  // Create NetTable if not already there.
  tables_lock_.acquireWriteLock();
  TableMap::iterator found = tables_.find(descriptor->name());
  if (found != tables_.end()) {
    LOG(WARNING) << "Table already defined! Checking consistency...";
    std::unique_ptr<NetTable> temp(new NetTable);
    temp->init(descriptor);
    std::shared_ptr<Revision> left = found->second->getTemplate(),
        right = temp->getTemplate();
    CHECK(right->structureMatch(*left));
  } else {
    // Storing as a pointer as NetTable memory position must not shift around
    // in memory.
    std::pair<TableMap::iterator, bool> inserted = tables_.insert(
        std::make_pair(descriptor->name(), std::unique_ptr<NetTable>()));
    CHECK(inserted.second) << tables_.size();
    inserted.first->second.reset(new NetTable);
    CHECK(inserted.first->second->init(descriptor));
  }
  tables_lock_.releaseWriteLock();

  // Ensure validity of table structure. May receive requests after this.
  bool first;
  PeerId entry_point;
  PeerIdList listeners;
  CHECK(syncTableDefinition(*descriptor_raw, &first, &entry_point, &listeners));

  // Join reference chord index.
  NetTable* table = &getTable(descriptor_raw->name());
  if (first) {
    table->createIndex();
  } else {
    table->joinIndex(entry_point);
  }

  // Join spatial chord index if applicable.
  if (descriptor_raw->spatial_extent_size() > 0) {
    CHECK_EQ(descriptor_raw->spatial_subdivision_size() * 2,
             descriptor_raw->spatial_extent_size());
    SpatialIndex::BoundingBox box;
    box.deserialize(descriptor_raw->spatial_extent());
    std::vector<size_t> subdivision(descriptor_raw->spatial_subdivision_size());
    for (int i = 0; i < descriptor_raw->spatial_subdivision_size(); ++i) {
      subdivision[i] = descriptor_raw->spatial_subdivision(i);
    }
    if (first) {
      table->createSpatialIndex(box, subdivision);
    } else {
      table->joinSpatialIndex(box, subdivision, entry_point);
    }
  }

  // Announce to listeners.
  table->announceToListeners(listeners);

  return table;
}

NetTable& NetTableManager::getTable(const std::string& name) {
  CHECK(Core::instance() != nullptr) << "Map API not initialized!";
  common::ScopedReadLock lock(&tables_lock_);
  TableMap::iterator found = tables_.find(name);
  // TODO(tcies) load table schema from metatable if not active
  CHECK(found != tables_.end()) << "Table not found: " << name;
  return *found->second;
}

const NetTable& NetTableManager::getTable(const std::string& name) const {
  CHECK(Core::instance() != nullptr) << "Map API not initialized!";
  tables_lock_.acquireReadLock();
  TableMap::const_iterator found = tables_.find(name);
  // TODO(tcies) load table schema from metatable if not active
  CHECK(found != tables_.end()) << "Table not found: " << name;
  tables_lock_.releaseReadLock();
  return *found->second;
}

bool NetTableManager::hasTable(const std::string& name) const {
  CHECK(Core::instance() != nullptr) << "Map API not initialized!";

  tables_lock_.acquireReadLock();
  bool has_table = tables_.count(name) > 0u;
  tables_lock_.releaseReadLock();
  return has_table;
}

void NetTableManager::tableList(std::vector<std::string>* tables) const {
  CHECK_NOTNULL(tables);
  tables->clear();
  common::ScopedReadLock lock(&tables_lock_);
  for (const std::pair<const std::string, std::unique_ptr<NetTable> >& pair :
       tables_) {
    tables->push_back(pair.first);
  }
}

void NetTableManager::listenToPeersJoiningTable(const std::string& table_name) {
  NetTable* metatable = &getTable(kMetaTableName);
  // TODO(tcies) Define default merging for metatable.
  while (true) {
    ChunkTransaction add_self_to_listeners(metatable_chunk_, metatable);
    std::shared_ptr<const Revision> current =
        add_self_to_listeners.findUnique(kMetaTableNameField, table_name);
    CHECK(current);
    proto::PeerList listeners;
    current->get(kMetaTableListenersField, &listeners);
    listeners.add_peers(Hub::instance().ownAddress());
    std::shared_ptr<Revision> next = current->copyForWrite();
    next->set(kMetaTableListenersField, listeners);
    add_self_to_listeners.update(next);
    if (add_self_to_listeners.commit()) {
      break;
    }
  }
}

void NetTableManager::kill() {
  tables_lock_.acquireReadLock();
  for (const std::pair<const std::string, std::unique_ptr<NetTable> >& table :
       tables_) {
    table.second->kill();
  }
  CHECK(tables_lock_.upgradeToWriteLock());
  tables_.clear();
  tables_lock_.releaseWriteLock();
}

void NetTableManager::killOnceShared() {
  tables_lock_.acquireReadLock();
  for (const std::pair<const std::string, std::unique_ptr<NetTable> >& table :
       tables_) {
    table.second->killOnceShared();
  }
  CHECK(tables_lock_.upgradeToWriteLock());
  tables_.clear();
  tables_lock_.releaseWriteLock();
}

NetTableManager::Iterator::Iterator(const TableMap::iterator& base,
                                    const TableMap& map)
    : base_(base), metatable_(map.find(kMetaTableName)) {
  CHECK(metatable_ != map.end());
  if (base_ == metatable_) {
    ++base_;
  }
}

NetTableManager::Iterator& NetTableManager::Iterator::operator++() {
  ++base_;
  if (base_ == metatable_) {
    ++base_;
  }
  return *this;
}

NetTable* NetTableManager::Iterator::operator*() { return base_->second.get(); }

bool NetTableManager::Iterator::operator!=(const Iterator& other) const {
  return other.base_ != base_;
}

// ========
// HANDLERS
// ========

void NetTableManager::handleConnectRequest(const Message& request,
                                           Message* response) {
  CHECK_NOTNULL(response);
  proto::ChunkRequestMetadata metadata;
  request.extract<LegacyChunk::kConnectRequest>(&metadata);
  const std::string& table = metadata.table();
  common::Id chunk_id(metadata.chunk_id());
  CHECK_NOTNULL(Core::instance());
  common::ScopedReadLock lock(&instance().tables_lock_);
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = instance().tables_.find(table);
  if (found == instance().tables_.end()) {
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleConnectRequest(chunk_id, PeerId(request.sender()),
                                      response);
}

void NetTableManager::handleInitRequest(
    const Message& request, Message* response) {
  proto::InitRequest init_request;
  request.extract<LegacyChunk::kInitRequest>(&init_request);
  TableMap::iterator found;
  if (getTableForRequestWithMetadataOrDecline(init_request, response, &found)) {
    found->second->handleInitRequest(init_request, PeerId(request.sender()),
                                     response);
  }
}

void NetTableManager::handleInsertRequest(
    const Message& request, Message* response) {
  proto::PatchRequest patch_request;
  request.extract<LegacyChunk::kInsertRequest>(&patch_request);
  TableMap::iterator found;
  if (getTableForRequestWithMetadataOrDecline(patch_request, response,
                                              &found)) {
    common::Id chunk_id(patch_request.metadata().chunk_id());
    std::shared_ptr<Revision> to_insert =
        Revision::fromProtoString(patch_request.serialized_revision());
    found->second->handleInsertRequest(chunk_id, to_insert, response);
  }
}

void NetTableManager::handleLeaveRequest(
    const Message& request, Message* response) {
  TableMap::iterator found;
  common::Id chunk_id;
  PeerId peer;
  if (getTableForMetadataRequestOrDecline<LegacyChunk::kLeaveRequest>(
          request, response, &found, &chunk_id, &peer)) {
    found->second->handleLeaveRequest(chunk_id, peer, response);
  }
}

void NetTableManager::handleLockRequest(
    const Message& request, Message* response) {
  TableMap::iterator found;
  common::Id chunk_id;
  PeerId peer;
  if (getTableForMetadataRequestOrDecline<LegacyChunk::kLockRequest>(
          request, response, &found, &chunk_id, &peer)) {
    found->second->handleLockRequest(chunk_id, peer, response);
  }
}

void NetTableManager::handleNewPeerRequest(
    const Message& request, Message* response) {
  proto::NewPeerRequest new_peer_request;
  request.extract<LegacyChunk::kNewPeerRequest>(&new_peer_request);
  TableMap::iterator found;
  if (getTableForRequestWithMetadataOrDecline(new_peer_request, response,
                                              &found)) {
    common::Id chunk_id(new_peer_request.metadata().chunk_id());
    PeerId new_peer(new_peer_request.new_peer()), sender(request.sender());
    found->second->handleNewPeerRequest(chunk_id, new_peer, sender, response);
  }
}

void NetTableManager::handleUnlockRequest(
    const Message& request, Message* response) {
  TableMap::iterator found;
  common::Id chunk_id;
  PeerId peer;
  if (getTableForMetadataRequestOrDecline<LegacyChunk::kUnlockRequest>(
          request, response, &found, &chunk_id, &peer)) {
    found->second->handleUnlockRequest(chunk_id, peer, response);
  }
}

void NetTableManager::handleUpdateRequest(
    const Message& request, Message* response) {
  proto::PatchRequest patch_request;
  request.extract<LegacyChunk::kUpdateRequest>(&patch_request);
  TableMap::iterator found;
  if (getTableForRequestWithMetadataOrDecline(patch_request, response,
                                              &found)) {
    common::Id chunk_id(patch_request.metadata().chunk_id());
    std::shared_ptr<Revision> to_insert =
        Revision::fromProtoString(patch_request.serialized_revision());
    PeerId sender(request.sender());
    found->second->handleUpdateRequest(chunk_id, to_insert, sender, response);
  }
}

void NetTableManager::handlePushNewChunksRequest(const Message& request,
                                                 Message* response) {
  CHECK_NOTNULL(response);
  TableMap::iterator found;
  PeerId listener;
  if (getTableForStringRequestOrDecline<NetTable::kPushNewChunksRequest>(
          request, response, &found, &listener)) {
    found->second->handleListenToChunksFromPeer(listener, response);
  }
}

void NetTableManager::handleAnnounceToListenersRequest(const Message& request,
                                                       Message* response) {
  CHECK_NOTNULL(response);
  TableMap::iterator found;
  PeerId announcer;
  if (getTableForStringRequestOrDecline<NetTable::kAnnounceToListeners>(
          request, response, &found, &announcer)) {
    found->second->handleAnnounceToListeners(announcer, response);
  }
}

void NetTableManager::handleSpatialTriggerNotification(const Message& request,
                                                       Message* response) {
  CHECK_NOTNULL(response);
  proto::SpatialIndexTrigger trigger;
  request.extract<SpatialIndex::kTriggerRequest>(&trigger);
  TableMap::iterator found;
  PeerId source;
  if (getTableForRequestWithStringOrDecline(trigger, response, &found)) {
    found->second->handleSpatialIndexTrigger(trigger);
    response->ack();
  }
}

void NetTableManager::handleRoutedNetTableChordRequests(const Message& request,
                                                        Message* response) {
  CHECK_NOTNULL(response);
  proto::RoutedChordRequest routed_request;
  request.extract<NetTableIndex::kRoutedChordRequest>(&routed_request);
  CHECK(routed_request.has_table_name());
  TableMap::iterator table;
  CHECK(findTable(routed_request.table_name(), &table));
  table->second->handleRoutedNetTableChordRequests(request, response);
}

void NetTableManager::handleRoutedSpatialChordRequests(const Message& request,
                                                       Message* response) {
  CHECK_NOTNULL(response);
  proto::RoutedChordRequest routed_request;
  request.extract<SpatialIndex::kRoutedChordRequest>(&routed_request);
  CHECK(routed_request.has_table_name());
  TableMap::iterator table;
  CHECK(findTable(routed_request.table_name(), &table));
  table->second->handleRoutedSpatialChordRequests(request, response);
}

void NetTableManager::handleRaftConnectRequest(const Message& request, Message* response) {
  proto::ChunkRequestMetadata metadata;
  request.extract<RaftNode::kConnectRequest>(&metadata);
  const std::string& table = metadata.table();
  common::Id chunk_id(metadata.chunk_id());
  // TODO(aqurai): CHECK_NOTNULL(Core::instance()) needed here?
  common::ScopedReadLock lock(&instance().tables_lock_);
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = instance().tables_.find(table);
  if (found == instance().tables_.end()) {
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleRaftConnectRequest(chunk_id, request.sender(), response);
}

void NetTableManager::handleRaftInitRequest(const Message& request, Message* response) {
  proto::InitRequest init_request;
  request.extract<RaftNode::kInitRequest>(&init_request);
  const proto::ChunkRequestMetadata metadata = init_request.metadata();
  const std::string& table = metadata.table();
  common::Id chunk_id(metadata.chunk_id());
  common::ScopedReadLock lock(&instance().tables_lock_);
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = instance().tables_.find(table);
  if (found == instance().tables_.end()) {
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleRaftInitRequest(chunk_id, init_request, request.sender(),
                                       response);
}

void NetTableManager::handleRaftAppendRequest(const Message& request, Message* response) {
  proto::AppendEntriesRequest append_request;
  request.extract<RaftNode::kAppendEntries>(&append_request);
  const proto::ChunkRequestMetadata metadata = append_request.metadata();
  const std::string& table = metadata.table();
  common::Id chunk_id(metadata.chunk_id());
  common::ScopedReadLock lock(&instance().tables_lock_);
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = instance().tables_.find(table);
  if (found == instance().tables_.end()) {
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleRaftAppendRequest(chunk_id, &append_request,
                                         request.sender(), response);
}

void NetTableManager::handleRaftChunkLockRequest(const Message& request,
                                                 Message* response) {
  proto::LockRequest lock_request;
  request.extract<RaftNode::kChunkLockRequest>(&lock_request);
  const proto::ChunkRequestMetadata metadata = lock_request.metadata();
  const std::string& table = metadata.table();
  common::Id chunk_id(metadata.chunk_id());
  common::ScopedReadLock lock(&instance().tables_lock_);
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = instance().tables_.find(table);
  if (found == instance().tables_.end()) {
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleRaftChunkLockRequest(chunk_id, request.sender(), response);
}

void NetTableManager::handleRaftChunkUnlockRequest(const Message& request,
                                                   Message* response) {
  proto::UnlockRequest unlock_request;
  request.extract<RaftNode::kChunkUnlockRequest>(&unlock_request);
  const proto::ChunkRequestMetadata metadata = unlock_request.metadata();
  const std::string& table = metadata.table();
  common::Id chunk_id(metadata.chunk_id());
  common::ScopedReadLock lock(&instance().tables_lock_);
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = instance().tables_.find(table);
  if (found == instance().tables_.end()) {
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleRaftChunkUnlockRequest(chunk_id, request.sender(),
                                            unlock_request.lock_entry_index(),
                                            unlock_request.proceed_commits(),
                                            response);
}

void NetTableManager::handleRaftInsertRequest(const Message& request, Message* response) {
  proto::InsertRequest insert_request;
  request.extract<RaftNode::kInsertRequest>(&insert_request);
  const proto::ChunkRequestMetadata metadata = insert_request.metadata();
  const std::string& table = metadata.table();
  common::Id chunk_id(metadata.chunk_id());
  common::ScopedReadLock lock(&instance().tables_lock_);
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = instance().tables_.find(table);
  if (found == instance().tables_.end()) {
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleRaftInsertRequest(chunk_id, &insert_request,
                                         request.sender(), response);
}

void NetTableManager::handleRaftUpdateRequest(const Message& request,
                                              Message* response) {
  proto::InsertRequest insert_request;
  request.extract<RaftNode::kUpdateRequest>(&insert_request);
  const proto::ChunkRequestMetadata metadata = insert_request.metadata();
  const std::string& table = metadata.table();
  common::Id chunk_id(metadata.chunk_id());
  common::ScopedReadLock lock(&instance().tables_lock_);
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator found =
      instance().tables_.find(table);
  if (found == instance().tables_.end()) {
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleRaftUpdateRequest(chunk_id, &insert_request,
                                         request.sender(), response);
}

void NetTableManager::handleRaftRequestVote(const Message& request, Message* response) {
  proto::VoteRequest vote_request;
  request.extract<RaftNode::kVoteRequest>(&vote_request);
  const proto::ChunkRequestMetadata metadata = vote_request.metadata();
  const std::string& table = metadata.table();
  common::Id chunk_id(metadata.chunk_id());
  common::ScopedReadLock lock(&instance().tables_lock_);
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = instance().tables_.find(table);
  if (found == instance().tables_.end()) {
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleRaftRequestVote(chunk_id, vote_request, request.sender(),
                                       response);
}

void NetTableManager::handleRaftJoinQuitRequest(const Message& request,
                                                Message* response) {
  proto::JoinQuitRequest join_quit_request;
  request.extract<RaftNode::kJoinQuitRequest>(&join_quit_request);
  const proto::ChunkRequestMetadata metadata = join_quit_request.metadata();
  const std::string& table = metadata.table();
  common::Id chunk_id(metadata.chunk_id());
  common::ScopedReadLock lock(&instance().tables_lock_);
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = instance().tables_.find(table);
  if (found == instance().tables_.end()) {
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleRaftJoinQuitRequest(chunk_id, join_quit_request,
                                           request.sender(), response);
}

void NetTableManager::handleRaftNotifyJoinQuitSuccess(const Message& request,
                                                      Message* response) {
  proto::NotifyJoinQuitSuccess notification;
  request.extract<RaftNode::kNotifyJoinQuitSuccess>(&notification);
  const proto::ChunkRequestMetadata metadata = notification.metadata();
  const std::string& table = metadata.table();
  common::Id chunk_id(metadata.chunk_id());
  common::ScopedReadLock lock(&instance().tables_lock_);
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = instance().tables_.find(table);
  if (found == instance().tables_.end()) {
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleRaftNotifyJoinQuitSuccess(chunk_id, notification,
                                                 response);
}

void NetTableManager::handleRaftQueryState(const Message& request,
                                           Message* response) {
  proto::QueryState query_state;
  request.extract<RaftNode::kQueryState>(&query_state);
  const proto::ChunkRequestMetadata metadata = query_state.metadata();
  const std::string& table = metadata.table();
  common::Id chunk_id(metadata.chunk_id());
  common::ScopedReadLock lock(&instance().tables_lock_);
  std::unordered_map<std::string, std::unique_ptr<NetTable> >::iterator
  found = instance().tables_.find(table);
  if (found == instance().tables_.end()) {
    response->impose<Message::kDecline>();
    return;
  }
  found->second->handleRaftQueryState(chunk_id, query_state, response);
}

bool NetTableManager::syncTableDefinition(const TableDescriptor& descriptor,
                                          bool* first, PeerId* entry_point,
                                          PeerIdList* listeners) {
  CHECK_NOTNULL(first);
  CHECK_NOTNULL(entry_point);
  CHECK_NOTNULL(listeners);
  CHECK_NOTNULL(metatable_chunk_);
  NetTable& metatable = getTable(kMetaTableName);

  // Assume that we are the first ones to define the table.
  ChunkTransaction try_insert(metatable_chunk_, &metatable);
  std::shared_ptr<Revision> attempt = metatable.getTemplate();
  common::Id metatable_id;
  common::generateId(&metatable_id);
  attempt->setId(metatable_id);
  attempt->set(kMetaTableNameField, descriptor.name());
  proto::PeerList peers;
  peers.add_peers(PeerId::self().ipPort());
  attempt->set(kMetaTableParticipantsField, peers);
  attempt->set(kMetaTableListenersField, proto::PeerList());
  attempt->set(kMetaTableStructureField, descriptor);
  try_insert.insert(attempt);
  try_insert.addConflictCondition(kMetaTableNameField, descriptor.name());

  if (try_insert.commit()) {
    *first = true;
    return true;
  } else {
    *first = false;
  }

  // Case Table definition already in metatable.
  ChunkTransaction try_join(metatable_chunk_, &metatable);
  // 1. Read previous registration in metatable.
  std::shared_ptr<const Revision> previous = try_join.findUnique(
      static_cast<int>(kMetaTableNameField), descriptor.name());
  CHECK(previous) << "Can't find table " << descriptor.name()
                  << " even though its presence seemingly caused a conflict.";
  // 2. Verify structure.
  TableDescriptor previous_descriptor;
  previous->get(kMetaTableStructureField, &previous_descriptor);
  CHECK_EQ(descriptor.SerializeAsString(),
           previous_descriptor.SerializeAsString());
  // 3. Pick entry point peer.
  previous->get(kMetaTableParticipantsField, &peers);
  CHECK_EQ(1, peers.peers_size()) << "Current implementation assumes only "
                                  << "one entry point peer per table.";
  *entry_point = PeerId(peers.peers(0));
  // 4. TODO(tcies) Register as peer.

  // 5. Grab listener peer ids.
  proto::PeerList listener_proto;
  previous->get(kMetaTableListenersField, &listener_proto);
  for (int i = 0; i < listener_proto.peers_size(); ++i) {
    listeners->push_back(PeerId(listener_proto.peers(i)));
  }

  // TODO(tcies) Commit registering as peer and remove table listeners
  // that are not reachable (merge while fail)?
  return true;
}

bool NetTableManager::findTable(const std::string& table_name,
                                TableMap::iterator* found) {
  CHECK_NOTNULL(found);
  common::ScopedReadLock lock(&instance().tables_lock_);
  *found = instance().tables_.find(table_name);
  if (*found == instance().tables_.end()) {
    return false;
  }
  return true;
}

} /* namespace map_api */
