#ifndef MAP_API_NET_TABLE_MANAGER_H_
#define MAP_API_NET_TABLE_MANAGER_H_

#include <memory>

#include "map-api/net-cr-table.h"
#include "map-api/table-descriptor.h"

namespace map_api {

class NetTableManager {
 public:
  /**
   * Mostly responsible for registering handlers.
   */
  void init();
  void addTable(std::unique_ptr<TableDescriptor>* descriptor);
  NetCRTable& getTable(const std::string& name);
  void clear();

  /**
   * ==========================
   * REQUEST HANDLERS AND TYPES
   * ==========================
   */
  /**
   * Requesting peer specifies which chunk it wants to connect to
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

 private:
  NetTableManager() = default;
  NetTableManager(const NetTableManager&) = delete;
  NetTableManager& operator =(const NetTableManager&) = delete;
  ~NetTableManager() = default;
  friend class MapApiCore;

  std::unordered_map<std::string, std::unique_ptr<NetCRTable> > tables_;
};

} /* namespace map_api */

#endif /* MAP_API_NET_TABLE_MANAGER_H_ */
