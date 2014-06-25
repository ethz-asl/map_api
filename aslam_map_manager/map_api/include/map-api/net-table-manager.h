#ifndef MAP_API_NET_TABLE_MANAGER_H_
#define MAP_API_NET_TABLE_MANAGER_H_

#include <memory>

#include "map-api/net-table.h"
#include "map-api/table-descriptor.h"

namespace map_api {

class NetTableManager {
 public:
  /**
   * Mostly responsible for registering handlers.
   */
  void init();
  void addTable(bool updateable, std::unique_ptr<TableDescriptor>* descriptor);
  /**
   * Can leave dangling reference
   */
  NetTable& getTable(const std::string& name);
  void leaveAllChunks();

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
  static void handleFindRequest(const std::string& serialized_request,
                                Message* response);
  static void handleInitRequest(const std::string& serialized_request,
                                Message* response);
  static void handleInsertRequest(const std::string& serialized_request,
                                  Message* response);
  static void handleLeaveRequest(const std::string& serialized_request,
                                 Message* response);
  static void handleLockRequest(const std::string& serialized_request,
                                Message* response);
  static void handleNewPeerRequest(const std::string& serialized_request,
                                   Message* response);
  static void handleUnlockRequest(const std::string& serialized_request,
                                  Message* response);
  static void handleUpdateRequest(const std::string& serialized_request,
                                  Message* response);

 private:
  NetTableManager() = default;
  NetTableManager(const NetTableManager&) = delete;
  NetTableManager& operator =(const NetTableManager&) = delete;
  ~NetTableManager() = default;
  friend class MapApiCore;

  typedef std::unordered_map<std::string, std::unique_ptr<NetTable> >
  TableMap;

  static bool routeChunkMetadataRequestOperations(
      const std::string& serialized_request, Message* response,
      TableMap::iterator* found, Id* chunk_id, PeerId* peer);

  template<typename RequestType>
  static bool routeChunkRequestOperations(
      const RequestType& request, Message* response,
      TableMap::iterator* found);

  /**
   * This function is necessary to keep MapApiCore out of the inlined
   * routeChunkRequestOperations(), to avoid circular includes.
   */
  static bool findTable(const std::string& table_name,
                        TableMap::iterator* found);

  TableMap tables_;
  Poco::RWLock tables_lock_;
};

} /* namespace map_api */

#include "map-api/net-table-manager-inl.h"

#endif /* MAP_API_NET_TABLE_MANAGER_H_ */
