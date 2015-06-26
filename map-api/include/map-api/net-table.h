#ifndef MAP_API_NET_TABLE_H_
#define MAP_API_NET_TABLE_H_

#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gtest/gtest_prod.h>
#include <multiagent-mapping-common/reader-first-reader-writer-lock.h>

#include "map-api/chunk-data-container-base.h"
#include "map-api/app-templates.h"
#include "map-api/chunk-base.h"
#include "map-api/net-table-index.h"
#include "map-api/spatial-index.h"
#include "./chunk.pb.h"
#include "./raft.pb.h"

DECLARE_bool(use_raft);

namespace map_api {
class ConstRevisionMap;
class MutableRevisionMap;

inline std::string humanReadableBytes(double size) {
  int i = 0;
  const char* units[] = {"B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};
  while (size > 1024) {
    size /= 1024.;
    ++i;
  }
  std::stringstream ss;
  ss << size << " " << units[i];
  return ss.str();
}

class NetTable {
  friend class ChunkTransaction;
  friend class NetTableFixture;
  friend class NetTableManager;
  friend class NetTableTransaction;
  friend class SpatialIndexTest;
  friend class Workspace;
  FRIEND_TEST(NetTableFixture, RemoteUpdate);
  FRIEND_TEST(NetTableFixture, Grind);
  FRIEND_TEST(NetTableFixture, SaveAndRestoreTableFromFile);

 public:
  static const std::string kChunkIdField;

  // BASICS
  const std::string& name() const;
  std::shared_ptr<Revision> getTemplate() const;

  // BASIC CHUNK MANAGEMENT
  ChunkBase* newChunk();
  ChunkBase* newChunk(const common::Id& chunk_id);
  ChunkBase* getChunk(const common::Id& chunk_id);

  // HIERARCHICAL CHUNK MANAGEMENT
  void pushNewChunkIdsToTracker(
      NetTable* table_of_tracking_item,
      const std::function<common::Id(const Revision&)>&
          how_to_determine_tracking_item);
  // In order to use this, an application should specialize determineTracker()
  // and tableForType() found in app-templates.h .
  template <typename TrackeeType, typename TrackerType, typename TrackerIdType>
  void pushNewChunkIdsToTracker();
  // If the transaction state is required for determining the id of the tracker,
  // (e.g. if tracker determination depends on other items), use this method,
  // then use Transaction::overrideTrackerIdentificationMethod() to set the
  // method to obtain the tracker for a given item.
  void pushNewChunkIdsToTracker(NetTable* table_of_tracking_item);
  // Attaches trigger involving fetchTrackedChunks() to updates of given item.
  // TODO(tcies) batch these for all followed items of the chunk?
  template <typename IdType>
  void followTrackedChunksOfItem(const IdType& item, ChunkBase* tracker_chunk);
  // Do the above automatically for all created and received items.
  void autoFollowTrackedChunks();

  // SPATIAL INDEX CHUNK MANAGEMENT
  void registerChunkInSpace(const common::Id& chunk_id,
                            const SpatialIndex::BoundingBox& bounding_box);
  template <typename IdType>
  void registerItemInSpace(const IdType& id,
                           const SpatialIndex::BoundingBox& bounding_box);
  void getChunkReferencesInBoundingBox(
      const SpatialIndex::BoundingBox& bounding_box,
      std::unordered_set<common::Id>* chunk_ids);
  void getChunksInBoundingBox(const SpatialIndex::BoundingBox& bounding_box);
  void getChunksInBoundingBox(const SpatialIndex::BoundingBox& bounding_box,
                              std::unordered_set<ChunkBase*>* chunks);
  inline SpatialIndex& spatial_index() {
    return *CHECK_NOTNULL(spatial_index_.get());
  }

  // TRIGGER RELATED
  typedef std::function<void(const std::unordered_set<common::Id>& insertions,
                             const std::unordered_set<common::Id>& updates,
                             ChunkBase* chunk)> TriggerCallbackWithChunkPointer;
  typedef std::function<void(ChunkBase* chunk)> ChunkAcquisitionCallback;
  // Will bind to Chunk* the pointer of the current chunk.
  void attachTriggerToCurrentAndFutureChunks(
      const TriggerCallbackWithChunkPointer& trigger);
  void attachCallbackToChunkAcquisition(
      const ChunkAcquisitionCallback& callback);
  // Returns false if peer not reachable.
  bool listenToChunksFromPeer(const PeerId& peer);
  void handleListenToChunksFromPeer(const PeerId& listener, Message* response);
  static const char kPushNewChunksRequest[];

  // ITEM RETRIEVAL
  // (locking all chunks)
  template <typename ValueType>
  void lockFind(int key, const ValueType& value, const LogicalTime& time,
                ConstRevisionMap* destination) const;
  void dumpActiveChunks(const LogicalTime& time, ConstRevisionMap* destination);
  void dumpActiveChunksAtCurrentTime(ConstRevisionMap* destination);
  template <typename IdType>
  void getAvailableIds(const LogicalTime& time,
                       std::vector<IdType>* ids);

  /**
   * Connects to the given chunk via the given peer.
   */
  ChunkBase* connectTo(const common::Id& chunk_id, const PeerId& peer);

  bool structureMatch(std::unique_ptr<TableDescriptor>* descriptor) const;

  size_t numActiveChunks() const;

  size_t numActiveChunksItems();

  size_t numItems() const;

  size_t activeChunksItemsSizeBytes();

  void shareAllChunks();

  void shareAllChunks(const PeerId& peer);

  void kill();

  // Make sure all chunks have at least one other peer.
  void killOnceShared();

  void leaveAllChunks();

  void forceStopAllRaftChunks();

  void leaveAllChunksOnceShared();

  std::string getStatistics();

  void getActiveChunkIds(std::set<common::Id>* chunk_ids) const;

  /**
   * Chunks are owned by the table, this function does not leak.
   */
  void getActiveChunks(std::set<ChunkBase*>* chunks) const;

  /**
   * ========================
   * Diverse request handlers
   * ========================
   * TODO(tcies) somehow unify all routing to chunks? (yes, like chord)
   */
  void handleConnectRequest(const common::Id& chunk_id, const PeerId& peer,
                            Message* response);
  void handleInitRequest(
      const proto::InitRequest& request, const PeerId& sender,
      Message* response);
  void handleInsertRequest(const common::Id& chunk_id,
                           const std::shared_ptr<Revision>& item,
                           Message* response);
  void handleLeaveRequest(
      const common::Id& chunk_id, const PeerId& leaver, Message* response);
  void handleLockRequest(
      const common::Id& chunk_id, const PeerId& locker, Message* response);
  void handleNewPeerRequest(
      const common::Id& chunk_id, const PeerId& peer, const PeerId& sender,
      Message* response);
  void handleUnlockRequest(
      const common::Id& chunk_id, const PeerId& locker, Message* response);
  void handleUpdateRequest(const common::Id& chunk_id,
                           const std::shared_ptr<Revision>& item,
                           const PeerId& sender, Message* response);

  void handleRoutedNetTableChordRequests(const Message& request,
                                         Message* response);
  void handleRoutedSpatialChordRequests(const Message& request,
                                        Message* response);

  void handleAnnounceToListeners(const PeerId& announcer,
                                 Message* response);
  static const char kAnnounceToListeners[];

  void handleSpatialIndexTrigger(const proto::SpatialIndexTrigger& trigger);

  // RaftChunk RPC handlers.
  void handleRaftConnectRequest(const common::Id& chunk_id,
                                const PeerId& sender, Message* response);
  void handleRaftInitRequest(const common::Id& chunk_id,
                             const proto::InitRequest& init_request,
                             const PeerId& sender, Message* response);
  void handleRaftChunkLockRequest(const common::Id& chunk_id,
                                  uint64_t serial_id, const PeerId& sender,
                                  Message* response);
  void handleRaftChunkUnlockRequest(const common::Id& chunk_id,
                                    uint64_t serial_id, const PeerId& sender,
                                    uint64_t lock_index, bool proceed_commits,
                                    Message* response);
  void handleRaftAppendRequest(const common::Id& chunk_id,
                               proto::AppendEntriesRequest* request,
                               const PeerId& sender, Message* response);
  void handleRaftInsertRequest(const common::Id& chunk_id,
                               proto::InsertRequest* request,
                               const PeerId& sender, Message* response);
  void handleRaftRequestVote(const common::Id& chunk_id,
                             const proto::VoteRequest& request,
                             const PeerId& sender, Message* response);
  void handleRaftQueryState(const common::Id& chunk_id,
                            const proto::QueryState& request,
                            Message* response);
  void handleRaftLeaveRequest(const common::Id& chunk_id, uint64_t serial_id,
                              const PeerId& sender, Message* response);
  void handleRaftLeaveNotification(const common::Id& chunk_id,
                                   Message* response);

  // Raft Multi-chunk commit RPCs
  void handleRaftChunkTransactionInfo(const common::Id& chunk_id,
                                      proto::ChunkTransactionInfo* info,
                                      const PeerId& sender, Message* response);
  void handleRaftQueryReadyToCommit(
      const common::Id& chunk_id,
      const proto::MultiChunkTransactionQuery& query, const PeerId& sender,
      Message* response);
  void handleRaftCommitNotification(
      const common::Id& chunk_id,
      const proto::MultiChunkTransactionQuery& query, const PeerId& sender,
      Message* response);
  void handleRaftAbortNotification(
      const common::Id& chunk_id,
      const proto::MultiChunkTransactionQuery& query, const PeerId& sender,
      Message* response);

 private:
  NetTable();
  NetTable(const NetTable&) = delete;
  NetTable& operator =(const NetTable&) = delete;

  bool init(std::shared_ptr<TableDescriptor> descriptor);

  // Interface for NetTableManager:
  void createIndex();
  void joinIndex(const PeerId& entry_point);
  void createSpatialIndex(const SpatialIndex::BoundingBox& bounds,
                          const std::vector<size_t>& subdivision);
  void joinSpatialIndex(const SpatialIndex::BoundingBox& bounds,
                        const std::vector<size_t>& subdivision,
                        const PeerId& entry_point);
  void announceToListeners(const PeerIdList& listeners);

  typedef std::unordered_map<common::Id, std::unique_ptr<ChunkBase>> ChunkMap;
  ChunkBase* addInitializedChunk(std::unique_ptr<ChunkBase>&& chunk);

  bool insert(const LogicalTime& time, ChunkBase* chunk,
              const std::shared_ptr<Revision>& query);
  /**
   * Must not change the chunk id. TODO(tcies) immutable fields of Revisions
   * could be nice and simple to implement
   */
  bool update(const std::shared_ptr<Revision>& query);
  /**
   * getById even though the corresponding chunk isn't locked
   * TODO(tcies) probably requires mutex on a data level
   */
  template <typename IdType>
  std::shared_ptr<const Revision> getById(const IdType& id,
                                          const LogicalTime& time);

  void readLockActiveChunks();
  void unlockActiveChunks();

  // Read-locks active_chunks_lock_ and passes each active chunk to action
  // individually.
  void forEachActiveChunk(
      const std::function<void(const ChunkBase& chunk)>& action) const;
  // Same as the above, but breaks if the function returns true.
  void forEachActiveChunkUntil(const std::function<
      bool(const ChunkBase& chunk)>& action) const;  // NOLINT

  bool routingBasics(
      const common::Id& chunk_id, Message* response, ChunkMap::iterator* found);

  typedef std::unordered_map<NetTable*,
      std::function<common::Id(const Revision&)> > NewChunkTrackerMap;
  inline const NewChunkTrackerMap& new_chunk_trackers() {
    return new_chunk_trackers_;
  }

  template <typename TrackeeType, typename TrackerType, typename TrackerIdType>
  std::function<common::Id(const Revision&)> trackerDeterminerFactory();

  void attachTriggers(ChunkBase* chunk);

  // Complements autoFollowTrackedChunks.
  void fetchAllCallback(const common::IdSet& insertions,
                        const common::IdSet& updates, ChunkBase* chunk);

  void leaveIndices();

  void getChunkHolders(const common::Id& chunk_id,
                       std::unordered_set<PeerId>* peers);
  void joinChunkHolders(const common::Id& chunk_id);
  void leaveChunkHolders(const common::Id& chunk_id);

  std::shared_ptr<TableDescriptor> descriptor_;
  ChunkMap active_chunks_;
  // See issue #2391 for why we need a reader-first RW mutex here.
  mutable common::ReaderFirstReaderWriterMutex active_chunks_lock_;

  // DO NOT USE FROM HANDLER THREAD (else TODO(tcies) mutex)
  std::unique_ptr<NetTableIndex> index_;
  std::unique_ptr<SpatialIndex> spatial_index_;
  common::ReaderWriterMutex index_lock_;

  std::vector<TriggerCallbackWithChunkPointer>
      triggers_to_attach_to_future_chunks_;
  std::mutex m_triggers_to_attach_;

  std::vector<ChunkAcquisitionCallback> chunk_acquisition_callbacks_;
  std::mutex m_chunk_acquisition_callbacks_;

  std::mutex m_new_chunk_listeners_;
  PeerIdSet new_chunk_listeners_;

  NewChunkTrackerMap new_chunk_trackers_;
};

}  // namespace map_api

#include "./net-table-inl.h"

#endif  // MAP_API_NET_TABLE_H_
