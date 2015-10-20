#ifndef MAP_API_TRANSACTION_INL_H_
#define MAP_API_TRANSACTION_INL_H_

#include <utility>
#include <vector>

#include <multiagent-mapping-common/accessors.h>

#include "map-api/chunk-manager.h"
#include "map-api/net-table-transaction-interface.h"
#include "map-api/object-cache.h"

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
std::shared_ptr<ObjectCache<IdType, ObjectType>> Transaction::createCache(
    NetTable* table) {
  CHECK_NOTNULL(table);
  std::shared_ptr<ObjectCache<IdType, ObjectType>> result(
      new ObjectCache<IdType, ObjectType>(this, table));
  CHECK(caches_.emplace(table, result).second);

  return result;
}

template <typename IdType, typename ObjectType>
const ObjectCache<IdType, ObjectType>& Transaction::getCache(NetTable* table) {
  CHECK_NOTNULL(table);
  // This ABSOLUTELY MUST REMAIN A DYNAMIC_CAST!!! Previously, it was static,
  // and resulted in a 3-day bug hunt.
  const ObjectCache<IdType, ObjectType>* result =
      dynamic_cast<const ObjectCache<IdType, ObjectType>*>(  // NOLINT
          common::getChecked(caches_, table).get());
  CHECK(result != nullptr) << "Requested cache type does not correspond to "
                           << "cache type previously created for table "
                           << table->name() << "!";
  return *result;
}

template <typename IdType>
std::shared_ptr<const Revision>* Transaction::getMutableUpdateEntry(
    const IdType& id, NetTable* table) {
  return transactionOf(CHECK_NOTNULL(table))->getMutableUpdateEntry(id);
}

}  // namespace map_api

#endif  // MAP_API_TRANSACTION_INL_H_
