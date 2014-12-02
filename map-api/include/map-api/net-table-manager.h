#ifndef MAP_API_NET_TABLE_MANAGER_H_
#define MAP_API_NET_TABLE_MANAGER_H_

#include <memory>
#include <string>
#include <vector>

#include <map-api/net-table.h>
#include <map-api/table-descriptor.h>

namespace map_api {

class NetTableManager {
 public:
  static const char kMetaTableName[];
  /**
   * Must be called before hub init
   */
  static void registerHandlers();

  /**
   * Singleton approach allows NetTableManager chord indices to communicate
   * before MapApiCore is fully initialized, which is an important part of
   * MapApiCore::init()
   */
  static NetTableManager& instance();

  void init(bool create_metatable_chunk);

  void initMetatable(bool create_metatable_chunk);

  NetTable* __attribute__((warn_unused_result))
      addTable(CRTable::Type type,
               std::unique_ptr<TableDescriptor>* descriptor);
  /**
   * Can leave dangling reference
   */
  NetTable& getTable(const std::string& name);

  void tableList(std::vector<std::string>* tables);

  void kill();

  /**
   * ==========================
   * REQUEST HANDLERS AND TYPES
   * ==========================
   */
  /**
   * Chunk requests
   */
  static void handleConnectRequest(const Message& request, Message* response);
  static void handleFindRequest(const Message& request, Message* response);
  static void handleInitRequest(const Message& request, Message* response);
  static void handleInsertRequest(const Message& request, Message* response);
  static void handleLeaveRequest(const Message& request, Message* response);
  static void handleLockRequest(const Message& request, Message* response);
  static void handleNewPeerRequest(const Message& request, Message* response);
  static void handleUnlockRequest(const Message& request, Message* response);
  static void handleUpdateRequest(const Message& request, Message* response);
  /**
   * Chord requests
   */
  static void handleRoutedNetTableChordRequests(const Message& request,
                                                Message* response);
  /**
   * Chord requests
   */
  static void handleRoutedSpatialChordRequests(const Message& request,
                                               Message* response);

 private:
  NetTableManager() = default;
  NetTableManager(const NetTableManager&) = delete;
  NetTableManager& operator =(const NetTableManager&) = delete;
  ~NetTableManager() = default;

  bool syncTableDefinition(
      CRTable::Type type, const TableDescriptor& descriptor, bool* first,
      PeerId* entry_point);

  typedef std::unordered_map<std::string, std::unique_ptr<NetTable> >
  TableMap;

  template<const char* request_type>
  static bool routeChunkMetadataRequestOperations(
      const Message& request, Message* response, TableMap::iterator* found,
      Id* chunk_id, PeerId* peer);

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

  Chunk* metatable_chunk_ = nullptr;

  TableMap tables_;
  Poco::RWLock tables_lock_;
};

}  // namespace map_api

#include "./net-table-manager-inl.h"

#endif  // MAP_API_NET_TABLE_MANAGER_H_
