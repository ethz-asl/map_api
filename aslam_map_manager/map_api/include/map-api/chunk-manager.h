#ifndef MAP_API_CHUNK_MANAGER_H_
#define MAP_API_CHUNK_MANAGER_H_

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <string>


#include "map-api/chunk.h"
#include "map-api/cr-table.h" // for singleton macros TODO(tcies) move

namespace map_api {

class ChunkManager {
 public:
  /**
   * Allows a peer to initiate a new chunk belonging to the given table
   */
  std::weak_ptr<Chunk> newChunk(const CRTable& table);
  /**
   * Looks for items among the peers and fetches chunks if they have matching
   * items.
   * Puts items into dest. As we assume full connectivity for now, this
   * basically requests all peers and collects the data from them.
   */
  int findAmongPeers(
      const CRTable& table, const std::string& key, const Revision& valueHolder,
      const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* dest);
  /**
   * Counterpart to findAmongPeers
   */
  static void handleFindRequest(const std::string& serialized_request,
                                zmq::socket_t* socket);
  /**
   * Returns singleton instance
   */
  static ChunkManager& instance();
 private:
  MAP_API_TABLE_SINGLETON_PATTERN_PROTECTED_METHODS(ChunkManager);
  /**
   * TODO(tcies) will probably become a LRU structure at some point
   */
  std::unordered_set<std::shared_ptr<Chunk> > active_chunks_;
};

} // namespace map_api

#endif /* MAP_API_CHUNK_MANAGER_H_ */
