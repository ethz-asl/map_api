#ifndef MAP_API_TRANSACTION_INL_H_
#define MAP_API_TRANSACTION_INL_H_

#include <utility>
#include <vector>

#include "map-api/chunk-manager.h"
#include "map-api/net-table-transaction-interface.h"

namespace map_api {

template <typename ValueType>
void Transaction::find(int key, const ValueType& value, NetTable* table,
                       ConstRevisionMap* result) {
  CHECK_NOTNULL(table);
  CHECK_NOTNULL(result);
  return this->transactionOf(table)->find(key, value, result);
}

template <typename IdType>
std::shared_ptr<const Revision> Transaction::getById(const IdType& id,
                                                     NetTable* table) const {
  CHECK_NOTNULL(table);
  return transactionOf(table)->getById(id);
}

template <typename IdType>
std::shared_ptr<const Revision> Transaction::getById(const IdType& id,
                                                     NetTable* table,
                                                     ChunkBase* chunk) const {
  CHECK_NOTNULL(table);
  CHECK_NOTNULL(chunk);
  return transactionOf(table)->getById(id, chunk);
}

template <typename IdType>
void Transaction::getAvailableIds(NetTable* table,
                                  std::vector<IdType>* ids) {
  return transactionOf(CHECK_NOTNULL(table))
      ->getAvailableIds(CHECK_NOTNULL(ids));
}

template <typename IdType>
void Transaction::remove(const IdType& id, NetTable* table) {
  return transactionOf(CHECK_NOTNULL(table))->remove(id);
}

template <typename TrackerIdType>
void Transaction::overrideTrackerIdentificationMethod(
    NetTable* trackee_table, NetTable* tracker_table,
    const std::function<TrackerIdType(const Revision&)>&
        how_to_determine_tracker) {
  CHECK_NOTNULL(trackee_table);
  CHECK_NOTNULL(tracker_table);
  CHECK(how_to_determine_tracker);
  enableDirectAccess();
  transactionOf(trackee_table)->overrideTrackerIdentificationMethod(
      tracker_table, how_to_determine_tracker);
  disableDirectAccess();
}

template <typename IdType, typename ObjectType>
std::shared_ptr<ObjectAndMetadataCache<IdType, ObjectType>>
Transaction::createCache(NetTable* table) {
  CHECK_NOTNULL(table);
  std::pair<CacheMap::iterator, bool> emplacement =
      caches_.emplace(table, CacheStruct());
  CHECK(emplacement.second);

  emplacement.first->second.chunk_manager.reset(
      new ChunkManagerChunkSize(kDefaultChunkSizeBytes, table));
  std::shared_ptr<ObjectAndMetadataCache<IdType, ObjectType>> result(
      new ObjectAndMetadataCache<IdType, ObjectType>(
          table,
          new NetTableTransactionInterface<IdType>(
              this, table, emplacement.first->second.chunk_manager.get())));
  emplacement.first->second.cache = result;

  return result;
}

template <typename IdType>
std::shared_ptr<const Revision>* Transaction::getMutableUpdateEntry(
    const IdType& id, NetTable* table) {
  return transactionOf(CHECK_NOTNULL(table))->getMutableUpdateEntry(id);
}

}  // namespace map_api

#endif  // MAP_API_TRANSACTION_INL_H_
