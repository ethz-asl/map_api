#ifndef MAP_API_NET_TABLE_H_
#define MAP_API_NET_TABLE_H_

#include <set>
#include <string>
#include <unordered_map>

#include <gtest/gtest_prod.h>
#include <Poco/RWLock.h>

#include "map-api/chunk.h"
#include "map-api/cr-table.h"
#include "map-api/net-table-index.h"
#include "map-api/revision.h"

namespace map_api {
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
  friend class NetTableTest;
  friend class NetTableTransaction;
  FRIEND_TEST(NetTableTest, RemoteUpdate);
  FRIEND_TEST(NetTableTest, Grind);
  FRIEND_TEST(NetTableTest, SaveAndRestoreTableFromFile);

 public:
  static const std::string kChunkIdField;

  bool init(CRTable::Type type, std::unique_ptr<TableDescriptor>* descriptor);
  void createIndex();
  void joinIndex(const PeerId& entry_point);

  const std::string& name() const;
  const CRTable::Type& type() const;

  std::shared_ptr<Revision> getTemplate() const;
  Chunk* newChunk();
  Chunk* newChunk(const Id& chunk_id);
  Chunk* getChunk(const Id& chunk_id);

  // RETRIEVAL (locking all chunks)
  template <typename ValueType>
  CRTable::RevisionMap lockFind(const std::string& key, const ValueType& value,
                                const LogicalTime& time);

  void dumpActiveChunks(const LogicalTime& time,
                        CRTable::RevisionMap* destination);
  void dumpActiveChunksAtCurrentTime(CRTable::RevisionMap* destination);

  /**
   * Connects to the given chunk via the given peer.
   */
  Chunk* connectTo(const Id& chunk_id, const PeerId& peer);

  bool structureMatch(std::unique_ptr<TableDescriptor>* descriptor) const;

  size_t numActiveChunks() const;

  size_t numActiveChunksItems();

  size_t activeChunksItemsSizeBytes();

  void shareAllChunks();

  void kill();

  void leaveAllChunks();

  std::string getStatistics();

  void getActiveChunkIds(std::set<Id>* chunk_ids) const;

  /**
   * Chunks are owned by the table, this function does not leak.
   */
  void getActiveChunks(std::set<Chunk*>* chunks) const;

  /**
   * ========================
   * Diverse request handlers
   * ========================
   * TODO(tcies) somehow unify all routing to chunks? (yes, like chord)
   */
  void handleConnectRequest(const Id& chunk_id, const PeerId& peer,
                            Message* response);
  void handleInitRequest(
      const proto::InitRequest& request, const PeerId& sender,
      Message* response);
  void handleInsertRequest(
      const Id& chunk_id, const Revision& item, Message* response);
  void handleLeaveRequest(
      const Id& chunk_id, const PeerId& leaver, Message* response);
  void handleLockRequest(
      const Id& chunk_id, const PeerId& locker, Message* response);
  void handleNewPeerRequest(
      const Id& chunk_id, const PeerId& peer, const PeerId& sender,
      Message* response);
  void handleUnlockRequest(
      const Id& chunk_id, const PeerId& locker, Message* response);
  void handleUpdateRequest(
      const Id& chunk_id, const Revision& item, const PeerId& sender,
      Message* response);

  void handleRoutedChordRequests(const Message& request, Message* response);

 private:
  NetTable();
  NetTable(const NetTable&) = delete;
  NetTable& operator =(const NetTable&) = delete;
  friend class NetTableManager;

  bool insert(Chunk* chunk, Revision* query);
  /**
   * Must not change the chunk id. TODO(tcies) immutable fields of Revisions
   * could be nice and simple to implement
   */
  bool update(Revision* query);
  template <typename IdType>
  std::shared_ptr<Revision> getByIdInconsistent(const IdType& id);

  void readLockActiveChunks();
  void unlockActiveChunks();

  typedef std::unordered_map<Id, std::unique_ptr<Chunk> > ChunkMap;
  bool routingBasics(
      const Id& chunk_id, Message* response, ChunkMap::iterator* found);

  CRTable::Type type_;
  std::unique_ptr<CRTable> cache_;
  ChunkMap active_chunks_;
  mutable Poco::RWLock active_chunks_lock_;
  // TODO(tcies) insert PeerHandler here

  /**
   * DO NOT USE FROM HANDLER THREAD (else TODO(tcies) mutex)
   */
  std::unique_ptr<NetTableIndex> index_;
  Poco::RWLock index_lock_;
};

}  // namespace map_api

#include "map-api/net-table-inl.h"

#endif  // MAP_API_NET_TABLE_H_
