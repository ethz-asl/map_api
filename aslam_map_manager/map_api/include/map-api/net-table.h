#ifndef MAP_API_NET_TABLE_H_
#define MAP_API_NET_TABLE_H_

#include <unordered_map>

#include "map-api/chunk.h"
#include "map-api/cr-table.h"
#include "map-api/revision.h"

namespace map_api {

class NetTable {
 public:
  static const std::string kChunkIdField;

  bool init(bool updateable, std::unique_ptr<TableDescriptor>* descriptor);
  const std::string& name() const;

  // INSERTION
  std::shared_ptr<Revision> getTemplate() const;
  Chunk* newChunk();
  Chunk* getChunk(const Id& chunk_id);
  bool insert(Chunk* chunk, Revision* query);
  /**
   * Must not change the chunk id. TODO(tcies) immutable fields of Revisions
   * could be nice and simple to implement
   */
  bool update(Revision* query);

  // RETRIEVAL
  std::shared_ptr<Revision> getById(const Id& id, const Time& time);
  /**
   * Finding: If can't find item locally, request at peers. There are subtleties
   * here: Is it enough to get data only from one chunk? I.e. shouldn't we
   * theoretically request data from all peers, even if we found some matching
   * items locally? Yes, we should - this would be horribly inefficient though.
   * Thus it would probably be better to expose two different
   * functions in the Net-CR-table: FastFind and ThoroughFind
   * (and of course FindUnique, which is a special case of FastFind). FastFind
   * would then only look until results from only one chunk have been found -
   * the chunk possibly already being held.
   * For the time being implementing only FastFind for simplicity.
   */
  template<typename ValueType>
  int findFast(
      const std::string& key, const ValueType& value, const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* destination);
  int findFastByRevision(
      const std::string& key, const Revision& valueHolder, const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* destination);
  template<typename ValueType>
  std::shared_ptr<Revision> findUnique(
      const std::string& key, const ValueType& value, const Time& time);
  void dumpCache(
      const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* destination);
  bool has(const Id& chunk_id);
  /**
   * Connects to the given chunk via the given peer.
   */
  Chunk* connectTo(const Id& chunk_id,
                                 const PeerId& peer);

  bool structureMatch(std::unique_ptr<TableDescriptor>* descriptor) const;

  void leaveAllChunks();

  /**
   * ========================
   * Diverse request handlers
   * ========================
   * TODO(tcies) somehow unify all routing to chunks?
   */
  void handleConnectRequest(const Id& chunk_id, const PeerId& peer,
                            Message* response);
  void handleInitRequest(
      const proto::InitRequest& request, Message* response);
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

 private:
  NetTable() = default;
  NetTable(const NetTable&) = delete;
  NetTable& operator =(const NetTable&) = delete;
  friend class NetTableManager;

  typedef std::unordered_map<Id, std::unique_ptr<Chunk> > ChunkMap;
  bool routingBasics(
      const Id& chunk_id, Message* response, ChunkMap::iterator* found);

  bool updateable_;
  std::unique_ptr<CRTable> cache_;
  ChunkMap active_chunks_;
  Poco::RWLock active_chunks_lock_;
  // TODO(tcies) insert PeerHandler here
};

} // namespace map_api

#include "map-api/net-table-inl.h"

#endif /* MAP_API_NET_TABLE_H_ */
