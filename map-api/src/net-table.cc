// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

#include <map-api/net-table.h>
#include <glog/logging.h>
#include <map-api/legacy-chunk-data-ram-container.h>
#include <map-api/legacy-chunk-data-stxxl-container.h>

#include <map-api-common/backtrace.h>

#include "map-api/core.h"
#include "map-api/hub.h"
#include "map-api/legacy-chunk.h"
#include "map-api/net-table-manager.h"
#include "map-api/transaction.h"

DEFINE_bool(use_raft, false, "Toggles use of Raft chunks.");

namespace map_api {

const std::string NetTable::kChunkIdField = "chunk_id";

const char NetTable::kPushNewChunksRequest[] = "map_api_net_table_push_new";
const char NetTable::kAnnounceToListeners[] =
    "map_api_net_table_announce_to_listeners";

MAP_API_STRING_MESSAGE(NetTable::kPushNewChunksRequest);
MAP_API_STRING_MESSAGE(NetTable::kAnnounceToListeners);

NetTable::NetTable() {}

bool NetTable::init(std::shared_ptr<TableDescriptor> descriptor) {
  descriptor_ = descriptor;
  return true;
}

void NetTable::createIndex() {
  map_api_common::ScopedWriteLock lock(&index_lock_);
  CHECK(index_.get() == nullptr);
  index_.reset(new NetTableIndex(name()));
  index_->create();
}

void NetTable::joinIndex(const PeerId& entry_point) {
  map_api_common::ScopedWriteLock lock(&index_lock_);
  CHECK(index_.get() == nullptr);
  index_.reset(new NetTableIndex(name()));
  index_->join(entry_point);
}

void NetTable::createSpatialIndex(const SpatialIndex::BoundingBox& bounds,
                                  const std::vector<size_t>& subdivision) {
  map_api_common::ScopedWriteLock lock(&index_lock_);
  CHECK(spatial_index_.get() == nullptr);
  spatial_index_.reset(new SpatialIndex(name(), bounds, subdivision));
  spatial_index_->create();
}

void NetTable::joinSpatialIndex(const SpatialIndex::BoundingBox& bounds,
                                const std::vector<size_t>& subdivision,
                                const PeerId& entry_point) {
  map_api_common::ScopedWriteLock lock(&index_lock_);
  CHECK(spatial_index_.get() == nullptr);
  spatial_index_.reset(new SpatialIndex(name(), bounds, subdivision));
  spatial_index_->join(entry_point);
}

void NetTable::announceToListeners(const PeerIdList& listeners) {
  for (const PeerId& peer : listeners) {
    Message request, response;
    request.impose<kAnnounceToListeners>(descriptor_->name());
    if (!Hub::instance().hasPeer(peer)) {
      LOG(ERROR) << "Host " << peer << " not among peers!";
      continue;
    }
    if (!Hub::instance().try_request(peer, &request, &response)) {
      // TODO(tcies) weed out unreachable peers.
      LOG(WARNING) << "Listener " << peer << " not reachable (any more?)!";
      continue;
    }
    CHECK(response.isOk());
  }
}

const std::string& NetTable::name() const { return descriptor_->name(); }

ChunkBase* NetTable::addInitializedChunk(std::unique_ptr<ChunkBase>&& chunk) {
  map_api_common::ScopedWriteLock lock(&active_chunks_lock_);
  std::pair<ChunkMap::iterator, bool> emplaced =
      active_chunks_.emplace(chunk->id(), std::move(chunk));
  CHECK(emplaced.second);
  ChunkBase* final_chunk_ptr = emplaced.first->second.get();
  // Attach triggers from triggers_to_attach_to_future_chunks_.
  attachTriggers(final_chunk_ptr);
  // Run callback for chunk acquisition.
  std::thread([this, final_chunk_ptr]() {
                std::lock_guard<std::mutex> lock(
                    m_chunk_acquisition_callbacks_);
                for (const ChunkAcquisitionCallback& callback :
                     chunk_acquisition_callbacks_) {
                  callback(final_chunk_ptr);
                }
              }).detach();
  return emplaced.first->second.get();
}

std::shared_ptr<Revision> NetTable::getTemplate() const {
  return descriptor_->getTemplate();
}

ChunkBase* NetTable::newChunk() {
  map_api_common::Id chunk_id;
  map_api_common::generateId(&chunk_id);
  return newChunk(chunk_id);
}

ChunkBase* NetTable::newChunk(const map_api_common::Id& chunk_id) {
  std::unique_ptr<ChunkBase> chunk;
  if (FLAGS_use_raft) {
    LOG(FATAL) << "TODO(aqurai): Implement raft chunk";
  } else {
    chunk.reset(new LegacyChunk);
  }
  chunk->initializeNew(chunk_id, descriptor_);
  ChunkBase* final_chunk_ptr = addInitializedChunk(std::move(chunk));

  joinChunkHolders(chunk_id);

  // Push chunk to listeners.
  std::lock_guard<std::mutex> l_new_chunk_listeners(m_new_chunk_listeners_);
  for (const PeerId& peer : new_chunk_listeners_) {
    if (final_chunk_ptr->requestParticipation(peer) == 0) {
      LOG(WARNING) << "Peer " << peer << ", who is listening to new chunks "
                   << " on " << name() << ", didn't receive new chunk!";
      // TODO(tcies) Find a good policy to remove stale listeners.
    }
  }
  return final_chunk_ptr;
}

ChunkBase* NetTable::getChunk(const map_api_common::Id& chunk_id) {
  active_chunks_lock_.acquireReadLock();
  ChunkMap::iterator found = active_chunks_.find(chunk_id);
  if (found == active_chunks_.end()) {
    // look in index and connect to peers that claim to have the data
    // (for now metatable only)
    std::unordered_set<PeerId> peers;
    getChunkHolders(chunk_id, &peers);
    // Chord can possibly be inconsistent, we therefore need to remove ourself
    // from the chunk holder list if we happen to be part of it.
    LOG_IF(WARNING, peers.erase(PeerId::self()) > 0u)
        << "Peer was falsely in holders of chunk " << chunk_id;
    CHECK(!peers.empty()) << "Chunk " << chunk_id.hexString()
                          << " not available!";
    active_chunks_lock_.releaseReadLock();
    connectTo(chunk_id, *peers.begin());
    active_chunks_lock_.acquireReadLock();
    found = active_chunks_.find(chunk_id);
    CHECK(found != active_chunks_.end());
  }
  ChunkBase* result = found->second.get();
  active_chunks_lock_.releaseReadLock();
  return result;
}

bool NetTable::ensureHasChunks(const map_api_common::IdSet& chunks_to_ensure) {
  bool success = true;
  for (const map_api_common::Id& chunk_id : chunks_to_ensure) {
    if (!getChunk(chunk_id)) {
      success = false;
      continue;
    }
  }
  return success;
}

void NetTable::pushNewChunkIdsToTracker(
    NetTable* table_of_tracking_item,
    const std::function<map_api_common::Id(const Revision&)>&
        how_to_determine_tracking_item) {
  CHECK_NOTNULL(table_of_tracking_item);
  CHECK(new_chunk_trackers_.insert(std::make_pair(
                                       table_of_tracking_item,
                                       how_to_determine_tracking_item)).second);
}

void NetTable::pushNewChunkIdsToTracker(NetTable* tracker_table) {
  CHECK_NOTNULL(tracker_table);
  auto identification_method_placeholder = [this, tracker_table](
      const Revision&) {
    LOG(FATAL) << "Override of tracker identification method (trackee = "
               << this->name() << ", tracker = " << tracker_table->name()
               << ") required!";
    return map_api_common::Id();
  };
  CHECK(new_chunk_trackers_.emplace(tracker_table,
                                    identification_method_placeholder).second);
}

template <>
void NetTable::followTrackedChunksOfItem(const map_api_common::Id& item_id,
                                         ChunkBase* tracker_chunk) {
  CHECK_NOTNULL(tracker_chunk);
  ChunkBase::TriggerCallback fetch_callback = [item_id, tracker_chunk, this](
      const map_api_common::IdSet& /*insertions*/, const map_api_common::IdSet& updates) {
    map_api_common::IdSet::const_iterator found = updates.find(item_id);
    if (found != updates.end()) {
      Transaction transaction;
      std::shared_ptr<const Revision> revision =
          transaction.getById(item_id, this, tracker_chunk);
      revision->fetchTrackedChunks();
    }
  };
  tracker_chunk->attachTrigger(fetch_callback);
  // Fetch tracked chunks now.
  fetch_callback(map_api_common::IdSet(), map_api_common::IdSet({item_id}));
}

void NetTable::autoFollowTrackedChunks() {
  // I suggest to leave this message here as a warning to future generations
  // that might have similarly terrible ideas.
  LOG(FATAL)
      << "autoFollowTrackedChunks is flawed since it can cause the "
      << "presence of an inconsistent set of chunks in concurrent views. "
      << "Revision::fetchTrackedChunks() should instead be called at the "
      << "beginning of transactions.";
}

const std::vector<Revision::AutoMergePolicy>& NetTable::getAutoMergePolicies()
    const {
  return auto_merge_policies_;
}

void NetTable::addAutoMergePolicy(
    const Revision::AutoMergePolicy& auto_merge_policy) {
  CHECK(auto_merge_policy);
  auto_merge_policies_.push_back(auto_merge_policy);
}

void NetTable::registerChunkInSpace(
    const map_api_common::Id& chunk_id, const SpatialIndex::BoundingBox& bounding_box) {
  active_chunks_lock_.acquireReadLock();
  CHECK(active_chunks_.find(chunk_id) != active_chunks_.end());
  active_chunks_lock_.releaseReadLock();
  map_api_common::ScopedReadLock lock(&index_lock_);
  spatial_index_->announceChunk(chunk_id, bounding_box);
}

void NetTable::getChunkReferencesInBoundingBox(
    const SpatialIndex::BoundingBox& bounding_box,
    std::unordered_set<map_api_common::Id>* chunk_ids) {
  CHECK_NOTNULL(chunk_ids);
  {
    map_api_common::ScopedReadLock lock(&index_lock_);
    spatial_index_->seekChunks(bounding_box, chunk_ids);
  }
}

void NetTable::getChunksInBoundingBox(
    const SpatialIndex::BoundingBox& bounding_box) {
  std::unordered_set<ChunkBase*> dummy;
  getChunksInBoundingBox(bounding_box, &dummy);
}

void NetTable::getChunksInBoundingBox(
    const SpatialIndex::BoundingBox& bounding_box,
    std::unordered_set<ChunkBase*>* chunks) {
  CHECK_NOTNULL(chunks);
  chunks->clear();
  std::unordered_set<map_api_common::Id> chunk_ids;
  getChunkReferencesInBoundingBox(bounding_box, &chunk_ids);
  for (const map_api_common::Id& id : chunk_ids) {
    ChunkBase* chunk = getChunk(id);
    CHECK_NOTNULL(chunk);
    chunks->insert(chunk);
  }
  VLOG(5) << "Got " << chunk_ids.size() << " chunks";
}

void NetTable::attachTriggerToCurrentAndFutureChunks(
    const TriggerCallbackWithChunkPointer& callback) {
  CHECK(callback);
  // Make sure no chunks are added during the execution of this.
  map_api_common::ScopedReadLock lock(&active_chunks_lock_);
  // Make sure future chunks will get the trigger attached.
  {
    std::lock_guard<std::mutex> attach_lock(m_triggers_to_attach_);
    triggers_to_attach_to_future_chunks_.push_back(callback);
  }
  // Attach trigger to all current chunks.
  for (const ChunkMap::value_type& id_chunk : active_chunks_) {
    ChunkBase* chunk = id_chunk.second.get();
    chunk->attachTrigger([chunk, callback](const map_api_common::IdSet& insertions,
                                           const map_api_common::IdSet& updates) {
      callback(insertions, updates, chunk);
    });
  }
}

void NetTable::attachCallbackToChunkAcquisition(
    const ChunkAcquisitionCallback& callback) {
  CHECK(callback);
  // Make sure no chunks are added during the execution of this.
  map_api_common::ScopedReadLock lock(&active_chunks_lock_);
  std::lock_guard<std::mutex> attach_lock(m_chunk_acquisition_callbacks_);
  chunk_acquisition_callbacks_.push_back(callback);
}

bool NetTable::listenToChunksFromPeer(const PeerId& peer) {
  Message request, response;
  request.impose<NetTable::kPushNewChunksRequest>(descriptor_->name());
  if (!Hub::instance().hasPeer(peer)) {
    LOG(ERROR) << "Peer with address " << peer << " not among peers!";
    return false;
  }
  Hub::instance().request(peer, &request, &response);
  if (!response.isOk()) {
    LOG(ERROR) << "Peer " << peer << " refused to share chunks!";
    return false;
  }
  return true;
}

void NetTable::handleListenToChunksFromPeer(const PeerId& listener,
                                            Message* response) {
  map_api_common::ScopedReadLock chunk_lock(&active_chunks_lock_);
  std::set<ChunkBase*> chunks_to_share_now;
  // Assumes read lock can be recursive (which it currently can).
  getActiveChunks(&chunks_to_share_now);

  std::lock_guard<std::mutex> l_new_chunk_listeners(m_new_chunk_listeners_);
  new_chunk_listeners_.emplace(listener);

  // Never call and RPC in an RPC handler.
  // Variables must be passed by copy, as they go out of scope.
  // Danger: Assumes chunks are not released in the meantime.
  // TODO(tcies) add a lock for removing chunks?
  std::thread previous_sharer([this, listener, chunks_to_share_now]() {
    for (ChunkBase* chunk : chunks_to_share_now) {
      CHECK_EQ(chunk->requestParticipation(listener), 1);
    }
  });
  previous_sharer.detach();

  response->ack();
}

bool NetTable::insert(const LogicalTime& time, ChunkBase* chunk,
                      const std::shared_ptr<Revision>& query) {
  CHECK_NOTNULL(chunk);
  CHECK(query != nullptr);
  CHECK(chunk->insert(time, query));
  return true;
}

bool NetTable::update(const std::shared_ptr<Revision>& query) {
  CHECK(query != nullptr);
  CHECK_NOTNULL(getChunk(query->getChunkId()))->update(query);
  return true;
}

void NetTable::dumpActiveChunks(const LogicalTime& time,
                                ConstRevisionMap* destination) {
  CHECK_NOTNULL(destination);
  destination->clear();
  std::set<map_api_common::Id> active_chunk_ids;
  getActiveChunkIds(&active_chunk_ids);
  for (const map_api_common::Id& chunk_id : active_chunk_ids) {
    ConstRevisionMap chunk_revisions;
    map_api::ChunkBase* chunk = getChunk(chunk_id);
    CHECK_NOTNULL(chunk);
    chunk->dumpItems(time, &chunk_revisions);
    destination->insert(chunk_revisions.begin(), chunk_revisions.end());
  }
}

void NetTable::dumpActiveChunksAtCurrentTime(ConstRevisionMap* destination) {
  CHECK_NOTNULL(destination);
  return dumpActiveChunks(map_api::LogicalTime::sample(), destination);
}

ChunkBase* NetTable::connectTo(const map_api_common::Id& chunk_id, const PeerId& peer) {
  Message request, response;
  // sends request of chunk info to peer
  proto::ChunkRequestMetadata metadata;
  metadata.set_table(descriptor_->name());
  chunk_id.serialize(metadata.mutable_chunk_id());
  request.impose<LegacyChunk::kConnectRequest>(metadata);
  // TODO(tcies) add to local peer subset as well?
  VLOG(5) << "Connecting to " << peer << " for chunk " << chunk_id;
  Hub::instance().request(peer, &request, &response);
  CHECK(response.isType<Message::kAck>()) << response.type();
  // wait for connect handle thread of other peer to succeed
  ChunkMap::iterator found;
  while (true) {
    active_chunks_lock_.acquireReadLock();
    found = active_chunks_.find(chunk_id);
    if (found != active_chunks_.end()) {
      active_chunks_lock_.releaseReadLock();
      break;
    }
    active_chunks_lock_.releaseReadLock();
    usleep(1000);
  }
  return found->second.get();
}

size_t NetTable::numActiveChunks() const {
  active_chunks_lock_.acquireReadLock();
  size_t result = active_chunks_.size();
  active_chunks_lock_.releaseReadLock();
  return result;
}

size_t NetTable::numActiveChunksItems() {
  std::set<map_api_common::Id> active_chunk_ids;
  getActiveChunkIds(&active_chunk_ids);
  size_t num_elements = 0;
  LogicalTime now = LogicalTime::sample();
  for (const map_api_common::Id& chunk_id : active_chunk_ids) {
    ChunkBase* chunk = getChunk(chunk_id);
    CHECK_NOTNULL(chunk);
    num_elements += chunk->numItems(now);
  }
  return num_elements;
}

size_t NetTable::numItems() const {
  size_t result = 0;
  LogicalTime count_time = LogicalTime::sample();
  forEachActiveChunk([&](const ChunkBase& chunk) {
    result += chunk.constData()->numAvailableIds(count_time);
  });
  return result;
}

size_t NetTable::activeChunksItemsSizeBytes() {
  std::set<map_api_common::Id> active_chunk_ids;
  getActiveChunkIds(&active_chunk_ids);
  size_t size_bytes = 0;
  LogicalTime now = LogicalTime::sample();
  for (const map_api_common::Id& chunk_id : active_chunk_ids) {
    ChunkBase* chunk = getChunk(chunk_id);
    CHECK_NOTNULL(chunk);
    size_bytes += chunk->itemsSizeBytes(now);
  }
  return size_bytes;
}

void NetTable::kill() {
  leaveAllChunks();
  leaveIndices();
}

void NetTable::killOnceShared() {
  leaveAllChunksOnceShared();
  leaveIndices();
}

void NetTable::shareAllChunks() {
  active_chunks_lock_.acquireReadLock();
  for (const ChunkMap::value_type& chunk : active_chunks_) {
    chunk.second->requestParticipation();
  }
  active_chunks_lock_.releaseReadLock();
}

void NetTable::shareAllChunks(const PeerId& peer) {
  active_chunks_lock_.acquireReadLock();
  for (const ChunkMap::value_type& chunk : active_chunks_) {
    chunk.second->requestParticipation(peer);
  }
  active_chunks_lock_.releaseReadLock();
}

void NetTable::leaveAllChunks() {
  active_chunks_lock_.acquireReadLock();
  for (const ChunkMap::value_type& chunk : active_chunks_) {
    chunk.second->leave();
    leaveChunkHolders(chunk.first);
  }
  CHECK(active_chunks_lock_.upgradeToWriteLock());
  active_chunks_.clear();
  active_chunks_lock_.releaseWriteLock();
}

void NetTable::leaveAllChunksOnceShared() {
  active_chunks_lock_.acquireReadLock();
  for (const ChunkMap::value_type& chunk : active_chunks_) {
    chunk.second->leaveOnceShared();
    leaveChunkHolders(chunk.first);
  }
  CHECK(active_chunks_lock_.upgradeToWriteLock());
  active_chunks_.clear();
  active_chunks_lock_.releaseWriteLock();
}

std::string NetTable::getStatistics() {
  std::stringstream ss;
  ss << name() << ": " << numActiveChunks() << " chunks and "
     << numActiveChunksItems() << " items. ["
     << humanReadableBytes(activeChunksItemsSizeBytes()) << "]";
  return ss.str();
}

void NetTable::getActiveChunkIds(std::set<map_api_common::Id>* chunk_ids) const {
  CHECK_NOTNULL(chunk_ids);
  chunk_ids->clear();
  active_chunks_lock_.acquireReadLock();
  for (const ChunkMap::value_type& chunk : active_chunks_) {
    chunk_ids->insert(chunk.first);
  }
  active_chunks_lock_.releaseReadLock();
}

void NetTable::getActiveChunks(std::set<ChunkBase*>* chunks) const {
  CHECK_NOTNULL(chunks);
  chunks->clear();
  active_chunks_lock_.acquireReadLock();
  for (const ChunkMap::value_type& chunk : active_chunks_) {
    chunks->insert(chunk.second.get());
  }
  active_chunks_lock_.releaseReadLock();
}

void NetTable::readLockActiveChunks() {
  active_chunks_lock_.acquireReadLock();
  for (const ChunkMap::value_type& chunk : active_chunks_) {
    chunk.second->readLock();
  }
}

void NetTable::unlockActiveChunks() {
  for (const ChunkMap::value_type& chunk : active_chunks_) {
    chunk.second->unlock();
  }
  active_chunks_lock_.releaseReadLock();
}

void NetTable::forEachActiveChunk(
    const std::function<void(const ChunkBase& chunk)>& action) const {
  map_api_common::ScopedReadLock lock(&active_chunks_lock_);
  for (const ChunkMap::value_type& chunk : active_chunks_) {
    action(*chunk.second);
  }
}

void NetTable::forEachActiveChunkUntil(const std::function<
    bool(const ChunkBase& chunk)>& action) const {  // NOLINT
  map_api_common::ScopedReadLock lock(&active_chunks_lock_);
  for (const ChunkMap::value_type& chunk : active_chunks_) {
    if (action(*chunk.second)) {
      break;
    }
  }
}

void NetTable::handleConnectRequest(const map_api_common::Id& chunk_id,
                                    const PeerId& peer, Message* response) {
  ChunkMap::iterator found;
  active_chunks_lock_.acquireReadLock();
  if (routingBasics(chunk_id, response, &found)) {
    LegacyChunk* chunk = CHECK_NOTNULL(
        dynamic_cast<LegacyChunk*>(found->second.get()));  // NOLINT
    chunk->handleConnectRequest(peer, response);
  }
  active_chunks_lock_.releaseReadLock();
}

void NetTable::handleInitRequest(const proto::InitRequest& request,
                                 const PeerId& sender, Message* response) {
  CHECK_NOTNULL(response);
  map_api_common::Id chunk_id(request.metadata().chunk_id());
  std::unique_ptr<LegacyChunk> chunk =
      std::unique_ptr<LegacyChunk>(new LegacyChunk);
  CHECK(chunk->init(chunk_id, request, sender, descriptor_));
  addInitializedChunk(std::move(chunk));
  response->ack();
  std::thread(&NetTable::joinChunkHolders, this, chunk_id).detach();
}

void NetTable::handleInsertRequest(const map_api_common::Id& chunk_id,
                                   const std::shared_ptr<Revision>& item,
                                   Message* response) {
  ChunkMap::iterator found;
  active_chunks_lock_.acquireReadLock();
  if (routingBasics(chunk_id, response, &found)) {
    LegacyChunk* chunk = CHECK_NOTNULL(
        dynamic_cast<LegacyChunk*>(found->second.get()));  // NOLINT
    chunk->handleInsertRequest(item, response);
  }
  active_chunks_lock_.releaseReadLock();
}

void NetTable::handleLeaveRequest(const map_api_common::Id& chunk_id,
                                  const PeerId& leaver, Message* response) {
  ChunkMap::iterator found;
  active_chunks_lock_.acquireReadLock();
  if (routingBasics(chunk_id, response, &found)) {
    LegacyChunk* chunk = CHECK_NOTNULL(
        dynamic_cast<LegacyChunk*>(found->second.get()));  // NOLINT
    chunk->handleLeaveRequest(leaver, response);
  }
  active_chunks_lock_.releaseReadLock();
}

void NetTable::handleLockRequest(const map_api_common::Id& chunk_id,
                                 const PeerId& locker, Message* response) {
  ChunkMap::iterator found;
  active_chunks_lock_.acquireReadLock();
  if (routingBasics(chunk_id, response, &found)) {
    LegacyChunk* chunk = CHECK_NOTNULL(
        dynamic_cast<LegacyChunk*>(found->second.get()));  // NOLINT
    chunk->handleLockRequest(locker, response);
  }
  active_chunks_lock_.releaseReadLock();
}

void NetTable::handleNewPeerRequest(const map_api_common::Id& chunk_id,
                                    const PeerId& peer, const PeerId& sender,
                                    Message* response) {
  ChunkMap::iterator found;
  active_chunks_lock_.acquireReadLock();
  if (routingBasics(chunk_id, response, &found)) {
    LegacyChunk* chunk = CHECK_NOTNULL(
        dynamic_cast<LegacyChunk*>(found->second.get()));  // NOLINT
    chunk->handleNewPeerRequest(peer, sender, response);
  }
  active_chunks_lock_.releaseReadLock();
}

void NetTable::handleUnlockRequest(const map_api_common::Id& chunk_id,
                                   const PeerId& locker, Message* response) {
  ChunkMap::iterator found;
  active_chunks_lock_.acquireReadLock();
  if (routingBasics(chunk_id, response, &found)) {
    LegacyChunk* chunk = CHECK_NOTNULL(
        dynamic_cast<LegacyChunk*>(found->second.get()));  // NOLINT
    chunk->handleUnlockRequest(locker, response);
  }
  active_chunks_lock_.releaseReadLock();
}

void NetTable::handleUpdateRequest(const map_api_common::Id& chunk_id,
                                   const std::shared_ptr<Revision>& item,
                                   const PeerId& sender, Message* response) {
  ChunkMap::iterator found;
  if (routingBasics(chunk_id, response, &found)) {
    LegacyChunk* chunk = CHECK_NOTNULL(
        dynamic_cast<LegacyChunk*>(found->second.get()));  // NOLINT
    chunk->handleUpdateRequest(item, sender, response);
  }
}

void NetTable::handleRoutedNetTableChordRequests(const Message& request,
                                                 Message* response) {
  map_api_common::ScopedReadLock lock(&index_lock_);
  if (index_) {
    index_->handleRoutedRequest(request, response);
  } else {
    response->decline();
  }
}

void NetTable::handleRoutedSpatialChordRequests(const Message& request,
                                                Message* response) {
  map_api_common::ScopedReadLock lock(&index_lock_);
  if (spatial_index_) {
    spatial_index_->handleRoutedRequest(request, response);
  } else {
    response->decline();
  }
}

void NetTable::handleAnnounceToListeners(const PeerId& announcer,
                                         Message* response) {
  // Never call an RPC in an RPC handler.
  std::thread(&NetTable::listenToChunksFromPeer, this, announcer).detach();
  response->ack();
}

void NetTable::handleSpatialIndexTrigger(
    const proto::SpatialIndexTrigger& trigger) {
  VLOG(5) << "Received spatial index trigger with " << trigger.new_chunks_size()
          << " new chunks";
  for (int i = 0; i < trigger.new_chunks_size(); ++i) {
    map_api_common::Id chunk_id(trigger.new_chunks(i));
    std::thread([this, chunk_id]() { CHECK_NOTNULL(getChunk(chunk_id)); })
        .detach();
  }
}

bool NetTable::routingBasics(const map_api_common::Id& chunk_id, Message* response,
                             ChunkMap::iterator* found) {
  CHECK_NOTNULL(response);
  CHECK_NOTNULL(found);
  *found = active_chunks_.find(chunk_id);
  if (*found == active_chunks_.end()) {
    LOG(WARNING) << "In " << name() << ", couldn't find " << chunk_id
                 << " among:";
    for (const ChunkMap::value_type& chunk : active_chunks_) {
      LOG(WARNING) << chunk.second->id();
    }
    response->impose<Message::kDecline>();
    return false;
  }
  return true;
}

void NetTable::attachTriggers(ChunkBase* chunk) {
  CHECK_NOTNULL(chunk);
  std::lock_guard<std::mutex> lock(m_triggers_to_attach_);
  if (!triggers_to_attach_to_future_chunks_.empty()) {
    for (const TriggerCallbackWithChunkPointer& trigger :
         triggers_to_attach_to_future_chunks_) {
      chunk->attachTrigger([trigger, chunk](const map_api_common::IdSet& insertions,
                                            const map_api_common::IdSet& updates) {
        trigger(insertions, updates, chunk);
      });
    }
  }
}

void NetTable::leaveIndices() {
  index_lock_.acquireReadLock();
  if (index_.get() != nullptr) {
    VLOG(1) << PeerId::self() << " leaving index for table " << name();
    index_->leave();
    CHECK(index_lock_.upgradeToWriteLock());
    index_.reset();
    index_lock_.releaseWriteLock();
  } else {
    index_lock_.releaseReadLock();
  }
  index_lock_.acquireReadLock();
  if (spatial_index_.get() != nullptr) {
    spatial_index_->leave();
    CHECK(index_lock_.upgradeToWriteLock());
    spatial_index_.reset();
    index_lock_.releaseWriteLock();
  } else {
    index_lock_.releaseReadLock();
  }
}

void NetTable::getChunkHolders(const map_api_common::Id& chunk_id,
                               std::unordered_set<PeerId>* peers) {
  CHECK_NOTNULL(peers);
  map_api_common::ScopedReadLock lock(&index_lock_);
  CHECK_NOTNULL(index_.get());
  index_->seekPeers(chunk_id, peers);
}

void NetTable::joinChunkHolders(const map_api_common::Id& chunk_id) {
  map_api_common::ScopedReadLock lock(&index_lock_);
  CHECK_NOTNULL(index_.get());
  VLOG(5) << "Joining " << chunk_id.hexString() << " holders";
  index_->announcePosession(chunk_id);
}

void NetTable::leaveChunkHolders(const map_api_common::Id& chunk_id) {
  map_api_common::ScopedReadLock lock(&index_lock_);
  CHECK_NOTNULL(index_.get());
  VLOG(5) << "Leaving " << chunk_id.hexString() << " holders";
  index_->renouncePosession(chunk_id);
}

}  // namespace map_api
