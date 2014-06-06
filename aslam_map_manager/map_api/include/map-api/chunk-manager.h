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
   * Registers handlers
   */
  bool init();

  /**
   * Connects to the given chunk via the given peer.
   */
  std::weak_ptr<Chunk> connectTo(const Id& chunk_id,
                                 const std::string& peer);
  /**
   * Allows a peer to initiate a new chunk belonging to the given table
   * TODO(tcies) ChunkManager should BELONG TO a table
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
   * Requests all peers in MapApiCore to participate in given chunk.
   * Returns how many peers accepted participation.
   * For the time being this causes the peers to send an independent connect
   * request, which should be handled by the requester before this function
   * returns (in the handler thread).
   * TODO(tcies) down the road, request only table peers
   * TODO(tcies) ability to respond with a request, instead of sending an
   * independent one?
   */
  int requestParticipation(const Chunk& chunk) const;

  /**
   * ==========================
   * REQUEST HANDLERS AND TYPES
   * ==========================
   */
  /**
   * Requesting peer specifies which chunk it want to connect to
   */
  static void handleConnectRequest(const std::string& serialized_request,
                                   Message* response);
  static const char kConnectRequest[];
  static const char kConnectResponse[];

  /**
   * Counterpart to findAmongPeers
   */
  static void handleFindRequest(const std::string& serialized_request,
                                zmq::socket_t* socket);

  /**
   * Requesting peer specifies which chunk it wants to add the data to and
   * appends the data.
   */
  static void handleInsertRequest(const std::string& serialized_request,
                                  Message* response);
  static const char kInsertRequest[]; // request type
  static const char kChunkNotOwned[]; // response type, also has kAck

  static void handleParticipationRequest(const std::string& serialized_request,
                                         Message* response);
  static const char kParticipationRequest[]; // request type
  // response types: Message::kAck, Message::kDecline

  static void handleLockRequest(const std::string& serialized_request,
                                Message* response);

  static void handleUnlockRequest(const std::string& serialized_request,
                                  Message* response);

  /**
   * Propagates removal of peers from the network.
   */
  static void handleRelinquishNotification(
      const std::string& serialized_notification);

  /**
   * Returns singleton instance
   */
  static ChunkManager& instance();
 private:
  MAP_API_TABLE_SINGLETON_PATTERN_PROTECTED_METHODS(ChunkManager);

  /**
   * TODO(tcies) will probably become a LRU structure at some point
   */
  typedef std::unordered_map<Id, std::shared_ptr<Chunk> > ChunkMap;
  ChunkMap active_chunks_;
};

} // namespace map_api

#endif /* MAP_API_CHUNK_MANAGER_H_ */
