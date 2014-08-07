#ifndef MAP_API_NET_TABLE_H_
#define MAP_API_NET_TABLE_H_

#include <unordered_map>

#include <Poco/RWLock.h>

#include "map-api/chunk.h"
#include "map-api/cr-table.h"
#include "map-api/net-table-index.h"
#include "map-api/revision.h"

namespace map_api {

class NetTable {
 public:
  static const std::string kChunkIdField;

  bool init(CRTable::Type type, std::unique_ptr<TableDescriptor>* descriptor);
  void createIndex();
  void joinIndex(const PeerId& entry_point);

  const std::string& name() const;

  std::shared_ptr<Revision> getTemplate() const;
  Chunk* newChunk();
  Chunk* newChunk(const Id& chunk_id);
  Chunk* getChunk(const Id& chunk_id);
  /**
   * Intended to be very temporary - bridges use of now removed
   * Transaction::find() in map_api_tango_interface
   */
  Chunk* getUniqueLocalChunk();

  bool insert(Chunk* chunk, Revision* query);
  /**
   * Must not change the chunk id. TODO(tcies) immutable fields of Revisions
   * could be nice and simple to implement
   */
  bool update(Revision* query);

  // RETRIEVAL
  /**
   * Deprecated - does not readlock chunks, nor does it guarantee consistency
   * in any other way (what if new data is added to table in an unowned chunk
   * that would still correspond to the query?). Function kept for
   * NetTableTest TODO(tcies) cleanup
   */
  std::shared_ptr<Revision> getById(const Id& id, const LogicalTime& time);
  /**
   * Deprecated for the same reasons
   */
  void dumpCache(
      const LogicalTime& time, CRTable::RevisionMap* destination);
  bool has(const Id& chunk_id);
  /**
   * Connects to the given chunk via the given peer.
   */
  Chunk* connectTo(const Id& chunk_id,
                   const PeerId& peer);

  bool structureMatch(std::unique_ptr<TableDescriptor>* descriptor) const;

  size_t activeChunksSize() const;

  size_t cachedItemsSize();

  void shareAllChunks();

  void kill();

  void leaveAllChunks();

  std::string getStatistics();

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

  typedef std::unordered_map<Id, std::unique_ptr<Chunk> > ChunkMap;
  bool routingBasics(
      const Id& chunk_id, Message* response, ChunkMap::iterator* found);

  CRTable::Type type_;
  std::unique_ptr<CRTable> cache_;
  ChunkMap active_chunks_;
  Poco::RWLock active_chunks_lock_;
  // TODO(tcies) insert PeerHandler here

  /**
   * DO NOT USE FROM HANDLER THREAD (else TODO(tcies) mutex)
   */
  std::unique_ptr<NetTableIndex> index_;
  Poco::RWLock index_lock_;
};

} // namespace map_api

#endif /* MAP_API_NET_TABLE_H_ */
