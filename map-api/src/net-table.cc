#include <map-api/net-table.h>
#include <glog/logging.h>

#include <statistics/statistics.h>
#include <timing/timer.h>

#include "map-api/core.h"
#include "map-api/cr-table-ram-map.h"
#include "map-api/cr-table-stxxl-map.h"
#include "map-api/cru-table-ram-map.h"
#include "map-api/cru-table-stxxl-map.h"
#include "map-api/hub.h"
#include "map-api/net-table-manager.h"

DEFINE_bool(use_external_memory, false, "External memory vs. RAM tables.");

namespace map_api {

const std::string NetTable::kChunkIdField = "chunk_id";

NetTable::NetTable() : type_(CRTable::Type::CR) {}

bool NetTable::init(
    CRTable::Type type, std::unique_ptr<TableDescriptor>* descriptor) {
  type_ = type;
  switch (type) {
    case CRTable::Type::CR:
      if (FLAGS_use_external_memory) {
        cache_.reset(new CRTableSTXXLMap);
      } else {
        cache_.reset(new CRTableRamMap);
      }
      break;
    case CRTable::Type::CRU:
      if (FLAGS_use_external_memory) {
        cache_.reset(new CRUTableSTXXLMap);
      } else {
        cache_.reset(new CRUTableRamMap);
      }
      break;
  }
  CHECK(cache_->init(descriptor));
  return true;
}

void NetTable::createIndex() {
  index_lock_.writeLock();
  CHECK(index_.get() == nullptr);
  index_.reset(new NetTableIndex(name()));
  index_->create();
  index_lock_.unlock();
}

void NetTable::joinIndex(const PeerId& entry_point) {
  index_lock_.writeLock();
  CHECK(index_.get() == nullptr);
  index_.reset(new NetTableIndex(name()));
  index_->join(entry_point);
  index_lock_.unlock();
}

void NetTable::createSpatialIndex(const SpatialIndex::BoundingBox& bounds,
                                  const std::vector<size_t>& subdivision) {
  index_lock_.writeLock();
  CHECK(spatial_index_.get() == nullptr);
  spatial_index_.reset(new SpatialIndex(name(), bounds, subdivision));
  spatial_index_->create();
  index_lock_.unlock();
}

void NetTable::joinSpatialIndex(const SpatialIndex::BoundingBox& bounds,
                                const std::vector<size_t>& subdivision,
                                const PeerId& entry_point) {
  index_lock_.writeLock();
  CHECK(spatial_index_.get() == nullptr);
  spatial_index_.reset(new SpatialIndex(name(), bounds, subdivision));
  spatial_index_->join(entry_point);
  index_lock_.unlock();
}

const std::string& NetTable::name() const {
  return cache_->name();
}

const CRTable::Type& NetTable::type() const { return type_; }

std::shared_ptr<Revision> NetTable::getTemplate() const {
  return cache_->getTemplate();
}

Chunk* NetTable::newChunk() {
  Id chunk_id;
  map_api::generateId(&chunk_id);
  return newChunk(chunk_id);
}

Chunk* NetTable::newChunk(const Id& chunk_id) {
  std::unique_ptr<Chunk> chunk = std::unique_ptr<Chunk>(new Chunk);
  CHECK(chunk->init(chunk_id, cache_.get(), true));
  if (trigger_to_attach_on_chunk_acquisition_) {
    auto trigger =
        std::bind(trigger_to_attach_on_chunk_acquisition_,
                  std::placeholders::_1, std::placeholders::_2, chunk.get());
    chunk->attachTrigger(trigger);
  }
  active_chunks_lock_.writeLock();
  std::pair<ChunkMap::iterator, bool> inserted =
      active_chunks_.insert(std::make_pair(chunk_id, std::unique_ptr<Chunk>()));
  CHECK(inserted.second) << "Chunk with id " << chunk_id << " already exists.";
  inserted.first->second = std::move(chunk);
  active_chunks_lock_.unlock();
  // add self to chunk posessors in index
  index_lock_.readLock();
  CHECK_NOTNULL(index_.get());
  index_->announcePosession(chunk_id);
  index_lock_.unlock();
  return inserted.first->second.get();
}

Chunk* NetTable::getChunk(const Id& chunk_id) {
  timing::Timer timer("map_api::NetTable::getChunk");
  active_chunks_lock_.readLock();
  ChunkMap::iterator found = active_chunks_.find(chunk_id);
  if (found == active_chunks_.end()) {
    // look in index and connect to peers that claim to have the data
    // (for now metatable only)
    std::unordered_set<PeerId> peers;
    index_lock_.readLock();
    CHECK_NOTNULL(index_.get());
    index_->seekPeers(chunk_id, &peers);
    index_lock_.unlock();
    CHECK_EQ(1u, peers.size()) << "Current implementation expects root only";
    active_chunks_lock_.unlock();
    connectTo(chunk_id, *peers.begin());
    active_chunks_lock_.readLock();
    found = active_chunks_.find(chunk_id);
    CHECK(found != active_chunks_.end());
  }
  Chunk* result = found->second.get();
  active_chunks_lock_.unlock();
  timer.Stop();
  return result;
}

void NetTable::registerChunkInSpace(
    const Id& chunk_id, const SpatialIndex::BoundingBox& bounding_box) {
  active_chunks_lock_.readLock();
  CHECK(active_chunks_.find(chunk_id) != active_chunks_.end());
  active_chunks_lock_.unlock();
  index_lock_.readLock();
  spatial_index_->announceChunk(chunk_id, bounding_box);
  index_lock_.unlock();
}

void NetTable::getChunkReferencesInBoundingBox(
    const SpatialIndex::BoundingBox& bounding_box,
    std::unordered_set<Id>* chunk_ids) {
  CHECK_NOTNULL(chunk_ids);
  timing::Timer seek_timer("map_api::NetTable::getChunksInBoundingBox - seek");
  index_lock_.readLock();
  spatial_index_->seekChunks(bounding_box, chunk_ids);
  index_lock_.unlock();
  seek_timer.Stop();
  statistics::StatsCollector collector(
      "map_api::NetTable::getChunksInBoundingBox - chunks");
  collector.AddSample(chunk_ids->size());
}

void NetTable::getChunksInBoundingBox(
    const SpatialIndex::BoundingBox& bounding_box) {
  std::unordered_set<Chunk*> dummy;
  getChunksInBoundingBox(bounding_box, &dummy);
}

void NetTable::getChunksInBoundingBox(
    const SpatialIndex::BoundingBox& bounding_box,
    std::unordered_set<Chunk*>* chunks) {
  CHECK_NOTNULL(chunks);
  chunks->clear();
  std::unordered_set<Id> chunk_ids;
  getChunkReferencesInBoundingBox(bounding_box, &chunk_ids);
  for (const Id& id : chunk_ids) {
    Chunk* chunk = getChunk(id);
    CHECK_NOTNULL(chunk);
    chunks->insert(chunk);
  }
  VLOG(3) << "Got " << chunk_ids.size() << " chunks";
}

void NetTable::attachTriggerOnChunkAcquisition(
    const TriggerCallbackWithChunkPointer& callback) {
  active_chunks_lock_.readLock();
  trigger_to_attach_on_chunk_acquisition_ = callback;
  active_chunks_lock_.unlock();
}

bool NetTable::insert(const LogicalTime& time, Chunk* chunk,
                      const std::shared_ptr<Revision>& query) {
  CHECK_NOTNULL(chunk);
  CHECK(query != nullptr);
  CHECK(chunk->insert(time, query));
  return true;
}

bool NetTable::update(const std::shared_ptr<Revision>& query) {
  CHECK(query != nullptr);
  CHECK(type_ == CRTable::Type::CRU);
  CHECK_NOTNULL(getChunk(query->getChunkId()))->update(query);
  return true;
}

void NetTable::dumpActiveChunks(const LogicalTime& time,
                                CRTable::RevisionMap* destination) {
  CHECK_NOTNULL(destination);
  destination->clear();
  std::set<map_api::Id> active_chunk_ids;
  getActiveChunkIds(&active_chunk_ids);
  for (const map_api::Id& chunk_id : active_chunk_ids) {
    map_api::CRTable::RevisionMap chunk_revisions;
    map_api::Chunk* chunk = getChunk(chunk_id);
    CHECK_NOTNULL(chunk);
    chunk->dumpItems(time, &chunk_revisions);
    destination->insert(chunk_revisions.begin(), chunk_revisions.end());
  }
}

void NetTable::dumpActiveChunksAtCurrentTime(
    CRTable::RevisionMap* destination) {
  CHECK_NOTNULL(destination);
  return dumpActiveChunks(map_api::LogicalTime::sample(), destination);
}

Chunk* NetTable::connectTo(const Id& chunk_id,
                           const PeerId& peer) {
  Message request, response;
  // sends request of chunk info to peer
  proto::ChunkRequestMetadata metadata;
  metadata.set_table(cache_->name());
  chunk_id.serialize(metadata.mutable_chunk_id());
  request.impose<Chunk::kConnectRequest>(metadata);
  // TODO(tcies) add to local peer subset as well?
  Hub::instance().request(peer, &request, &response);
  CHECK(response.isType<Message::kAck>()) << response.type();
  // wait for connect handle thread of other peer to succeed
  ChunkMap::iterator found;
  while (true) {
    active_chunks_lock_.readLock();
    found = active_chunks_.find(chunk_id);
    if (found != active_chunks_.end()) {
      active_chunks_lock_.unlock();
      break;
    }
    active_chunks_lock_.unlock();
    usleep(1000);
  }
  return found->second.get();
}

size_t NetTable::numActiveChunks() const {
  active_chunks_lock_.readLock();
  size_t result = active_chunks_.size();
  active_chunks_lock_.unlock();
  return result;
}

size_t NetTable::numActiveChunksItems() {
  std::set<Id> active_chunk_ids;
  getActiveChunkIds(&active_chunk_ids);
  size_t num_elements = 0;
  LogicalTime now = LogicalTime::sample();
  for (const Id& chunk_id : active_chunk_ids) {
    Chunk* chunk = getChunk(chunk_id);
    CHECK_NOTNULL(chunk);
    num_elements += chunk->numItems(now);
  }
  return num_elements;
}

size_t NetTable::numItems() {
  return cache_->count(-1, 0, LogicalTime::sample());
}

size_t NetTable::activeChunksItemsSizeBytes() {
  std::set<Id> active_chunk_ids;
  getActiveChunkIds(&active_chunk_ids);
  size_t size_bytes = 0;
  LogicalTime now = LogicalTime::sample();
  for (const Id& chunk_id : active_chunk_ids) {
    Chunk* chunk = getChunk(chunk_id);
    CHECK_NOTNULL(chunk);
    size_bytes += chunk->itemsSizeBytes(now);
  }
  return size_bytes;
}

void NetTable::kill() {
  leaveAllChunks();
  index_lock_.readLock();
  if (index_.get() != nullptr) {
    index_->leave();
    index_lock_.unlock();
    index_lock_.writeLock();
    index_.reset();
  }
  index_lock_.unlock();
  index_lock_.readLock();
  if (spatial_index_.get() != nullptr) {
    spatial_index_->leave();
    index_lock_.unlock();
    index_lock_.writeLock();
    spatial_index_.reset();
  }
  index_lock_.unlock();
}

void NetTable::shareAllChunks() {
  active_chunks_lock_.readLock();
  for (const std::pair<const Id, std::unique_ptr<Chunk> >& chunk :
      active_chunks_) {
    chunk.second->requestParticipation();
  }
  active_chunks_lock_.unlock();
}

void NetTable::shareAllChunks(const PeerId& peer) {
  active_chunks_lock_.readLock();
  for (const std::pair<const Id, std::unique_ptr<Chunk> >& chunk :
       active_chunks_) {
    chunk.second->requestParticipation(peer);
  }
  active_chunks_lock_.unlock();
}

void NetTable::leaveAllChunks() {
  active_chunks_lock_.readLock();
  for (const std::pair<const Id, std::unique_ptr<Chunk> >& chunk :
      active_chunks_) {
    chunk.second->leave();
  }
  active_chunks_lock_.unlock();
  active_chunks_lock_.writeLock();
  active_chunks_.clear();
  active_chunks_lock_.unlock();
  cache_->clear();
}

std::string NetTable::getStatistics() {
  std::stringstream ss;
  ss << name() << ": " << numActiveChunks() << " chunks and "
     << numActiveChunksItems() << " items. ["
     << humanReadableBytes(activeChunksItemsSizeBytes()) << "]";
  return ss.str();
}

void NetTable::getActiveChunkIds(std::set<Id>* chunk_ids) const {
  CHECK_NOTNULL(chunk_ids);
  chunk_ids->clear();
  active_chunks_lock_.readLock();
  for (const std::pair<const Id, std::unique_ptr<Chunk> >& chunk :
       active_chunks_) {
    chunk_ids->insert(chunk.first);
  }
  active_chunks_lock_.unlock();
}

void NetTable::getActiveChunks(std::set<Chunk*>* chunks) const {
  CHECK_NOTNULL(chunks);
  chunks->clear();
  active_chunks_lock_.readLock();
  for (const std::pair<const Id, std::unique_ptr<Chunk> >& chunk :
       active_chunks_) {
    chunks->insert(chunk.second.get());
  }
  active_chunks_lock_.unlock();
}

void NetTable::readLockActiveChunks() {
  active_chunks_lock_.readLock();
  for (const ChunkMap::value_type& chunk : active_chunks_) {
    chunk.second->readLock();
  }
}

void NetTable::unlockActiveChunks() {
  for (const ChunkMap::value_type& chunk : active_chunks_) {
    chunk.second->unlock();
  }
  active_chunks_lock_.unlock();
}

void NetTable::handleConnectRequest(const Id& chunk_id, const PeerId& peer,
                                    Message* response) {
  ChunkMap::iterator found;
  active_chunks_lock_.readLock();
  if (routingBasics(chunk_id, response, &found)) {
    found->second->handleConnectRequest(peer, response);
  }
  active_chunks_lock_.unlock();
}

void NetTable::handleInitRequest(
    const proto::InitRequest& request, const PeerId& sender,
    Message* response) {
  CHECK_NOTNULL(response);
  Id chunk_id(request.metadata().chunk_id());
  std::unique_ptr<Chunk> chunk = std::unique_ptr<Chunk>(new Chunk);
  CHECK(chunk->init(chunk_id, request, sender, cache_.get()));
  if (trigger_to_attach_on_chunk_acquisition_) {
    auto trigger =
        std::bind(trigger_to_attach_on_chunk_acquisition_,
                  std::placeholders::_1, std::placeholders::_2, chunk.get());
    chunk->attachTrigger(trigger);
  }
  active_chunks_lock_.writeLock();
  std::pair<ChunkMap::iterator, bool> inserted =
      active_chunks_.insert(std::make_pair(chunk_id, std::unique_ptr<Chunk>()));
  CHECK(inserted.second);
  inserted.first->second = std::move(chunk);
  active_chunks_lock_.unlock();
  response->ack();
}

void NetTable::handleInsertRequest(const Id& chunk_id,
                                   const std::shared_ptr<Revision>& item,
                                   Message* response) {
  ChunkMap::iterator found;
  active_chunks_lock_.readLock();
  if (routingBasics(chunk_id, response, &found)) {
    found->second->handleInsertRequest(item, response);
  }
  active_chunks_lock_.unlock();
}

void NetTable::handleLeaveRequest(
    const Id& chunk_id, const PeerId& leaver, Message* response) {
  ChunkMap::iterator found;
  active_chunks_lock_.readLock();
  if (routingBasics(chunk_id, response, &found)) {
    found->second->handleLeaveRequest(leaver, response);
  }
  active_chunks_lock_.unlock();
}

void NetTable::handleLockRequest(
    const Id& chunk_id, const PeerId& locker, Message* response) {
  ChunkMap::iterator found;
  active_chunks_lock_.readLock();
  if (routingBasics(chunk_id, response, &found)) {
    found->second->handleLockRequest(locker, response);
  }
  active_chunks_lock_.unlock();
}

void NetTable::handleNewPeerRequest(
    const Id& chunk_id, const PeerId& peer, const PeerId& sender,
    Message* response) {
  ChunkMap::iterator found;
  active_chunks_lock_.readLock();
  if (routingBasics(chunk_id, response, &found)) {
    found->second->handleNewPeerRequest(peer, sender, response);
  }
  active_chunks_lock_.unlock();
}

void NetTable::handleUnlockRequest(
    const Id& chunk_id, const PeerId& locker, Message* response) {
  ChunkMap::iterator found;
  active_chunks_lock_.readLock();
  if (routingBasics(chunk_id, response, &found)) {
    found->second->handleUnlockRequest(locker, response);
  }
  active_chunks_lock_.unlock();
}

void NetTable::handleUpdateRequest(const Id& chunk_id,
                                   const std::shared_ptr<Revision>& item,
                                   const PeerId& sender, Message* response) {
  ChunkMap::iterator found;
  if (routingBasics(chunk_id, response, &found)) {
    found->second->handleUpdateRequest(item, sender, response);
  }
}

void NetTable::handleRoutedNetTableChordRequests(const Message& request,
                                                 Message* response) {
  index_lock_.readLock();
  CHECK_NOTNULL(index_.get());
  index_->handleRoutedRequest(request, response);
  index_lock_.unlock();
}

void NetTable::handleRoutedSpatialChordRequests(const Message& request,
                                                Message* response) {
  index_lock_.readLock();
  CHECK_NOTNULL(spatial_index_.get());
  spatial_index_->handleRoutedRequest(request, response);
  index_lock_.unlock();
}

bool NetTable::routingBasics(
    const Id& chunk_id, Message* response, ChunkMap::iterator* found) {
  CHECK_NOTNULL(response);
  CHECK_NOTNULL(found);
  *found = active_chunks_.find(chunk_id);
  if (*found == active_chunks_.end()) {
    LOG(WARNING) << "Couldn't find " << chunk_id << " among:";
    for (const ChunkMap::value_type& chunk : active_chunks_) {
      LOG(WARNING) << chunk.second->id();
    }
    response->impose<Message::kDecline>();
    return false;
  }
  return true;
}

}  // namespace map_api
