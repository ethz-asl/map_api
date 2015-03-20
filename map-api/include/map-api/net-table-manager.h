#ifndef MAP_API_NET_TABLE_MANAGER_H_
#define MAP_API_NET_TABLE_MANAGER_H_

#include <memory>
#include <string>
#include <vector>

#include "map-api/net-table.h"
#include "map-api/reader-writer-lock.h"
#include "map-api/table-descriptor.h"

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

  void listenToPeersJoiningTable(const std::string& table_name);

  void kill();

  typedef std::unordered_map<std::string, std::unique_ptr<NetTable> > TableMap;
  // Need custom iterator to skip metatable, which is not supposed to be part of
  // the iteration.
  class Iterator {
   public:
    Iterator(const TableMap::iterator& base, const TableMap& map);
    Iterator& operator++();
    NetTable* operator*();
    bool operator!=(const Iterator& other) const;

   private:
    TableMap::iterator base_;
    const TableMap::const_iterator metatable_;
  };
  // Not thread-safe, assumes table initialization has happened before.
  inline Iterator begin() { return Iterator(tables_.begin(), tables_); }
  inline Iterator end() { return Iterator(tables_.end(), tables_); }

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
   * Net table requests
   */
  static void handlePushNewChunksRequest(const Message& request,
                                         Message* response);
  static void handleAnnounceToListenersRequest(const Message& request,
                                               Message* response);
  static void handleSpatialTriggerNotification(const Message& request,
                                               Message* response);
  /**
   * Chord requests
   */
  static void handleRoutedNetTableChordRequests(const Message& request,
                                                Message* response);
  static void handleRoutedSpatialChordRequests(const Message& request,
                                               Message* response);

 private:
  NetTableManager();
  NetTableManager(const NetTableManager&) = delete;
  NetTableManager& operator =(const NetTableManager&) = delete;
  ~NetTableManager() = default;

  bool syncTableDefinition(CRTable::Type type,
                           const TableDescriptor& descriptor, bool* first,
                           PeerId* entry_point, PeerIdList* listeners);

  template <const char* RequestType>
  static bool getTableForMetadataRequestOrDecline(const Message& request,
                                                  Message* response,
                                                  TableMap::iterator* found,
                                                  common::Id* chunk_id,
                                                  PeerId* peer);
  template <const char* RequestType>
  static bool getTableForStringRequestOrDecline(const Message& request,
                                                Message* response,
                                                TableMap::iterator* found,
                                                PeerId* peer);
  template <typename MetadataRequestType>
  static bool getTableForRequestWithMetadataOrDecline(
      const MetadataRequestType& request, Message* response,
      TableMap::iterator* found);
  template <typename StringRequestType>
  static bool getTableForRequestWithStringOrDecline(
      const StringRequestType& request, Message* response,
      TableMap::iterator* found);

  /**
   * This function is necessary to keep MapApiCore out of the inlined
   * routeChunkRequestOperations(), to avoid circular includes.
   */
  static bool findTable(const std::string& table_name,
                        TableMap::iterator* found);

  Chunk* metatable_chunk_;

  TableMap tables_;
  ReaderWriterMutex tables_lock_;

  NetTable* metatable_;
};

}  // namespace map_api

#include "./net-table-manager-inl.h"

#endif  // MAP_API_NET_TABLE_MANAGER_H_
