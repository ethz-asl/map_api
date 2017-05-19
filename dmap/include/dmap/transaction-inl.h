#ifndef DMAP_TRANSACTION_INL_H_
#define DMAP_TRANSACTION_INL_H_

#include <string>
#include <utility>
#include <vector>

#include <dmap-common/accessors.h>

#include "dmap/chunk-manager.h"
#include "dmap/conflicts.h"
#include "dmap/net-table-transaction-interface.h"
#include "dmap/threadsafe-cache.h"

namespace dmap {

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
void Transaction::getAvailableIds(NetTable* table, std::vector<IdType>* ids) {
  return transactionOf(CHECK_NOTNULL(table))
      ->getAvailableIds(CHECK_NOTNULL(ids));
}

template <typename IdType>
void Transaction::fetchAllChunksTrackedBy(const IdType& id,
                                          NetTable* const table) {
  CHECK_NOTNULL(table);

  enableDirectAccess();
  std::shared_ptr<const Revision> tracker = getById(id, table);
  CHECK(tracker);
  disableDirectAccess();

  CHECK(tracker->fetchTrackedChunks());

  refreshIdToChunkIdMaps();
  // Id to chunk id maps must be refreshed first, otherwise getAvailableIds will
  // nor work.
  refreshAvailableIdsInCaches();
}

template <typename IdType>
void Transaction::remove(const IdType& id, NetTable* table) {
  CHECK(!finalized_);
  return transactionOf(CHECK_NOTNULL(table))->remove(id);
}

template <typename ObjectType>
std::string Transaction::debugConflictsInTable(NetTable* table) {
  CHECK_NOTNULL(table);
  std::shared_ptr<Transaction> dummy(new Transaction);
  ConflictMap conflicts;
  merge(dummy, &conflicts);
  return conflicts.debugConflictsInTable<ObjectType>(table);
}

template <typename IdType, typename ObjectType>
std::shared_ptr<ThreadsafeCache<IdType, ObjectType>> Transaction::createCache(
    NetTable* table) {
  CHECK_NOTNULL(table);
  CHECK(!finalized_);
  std::shared_ptr<ThreadsafeCache<IdType, ObjectType>> result(
      new ThreadsafeCache<IdType, ObjectType>(this, table));
  CHECK(caches_.emplace(table, result).second);
  return result;
}

template <typename IdType, typename ObjectType>
const ThreadsafeCache<IdType, ObjectType>& Transaction::getCache(
    NetTable* table) {
  return *getMutableCache<IdType, ObjectType>(table);
}

template <typename IdType, typename ObjectType>
void Transaction::setCacheUpdateFilter(
    const std::function<bool(const ObjectType& original,  // NOLINT
                             const ObjectType& innovation)>& update_filter,
    NetTable* table) {
  CHECK_NOTNULL(table);
  CHECK(!finalized_);
  ThreadsafeCache<IdType, ObjectType>* cache =
      getMutableCache<IdType, ObjectType>(table);
  cache->setUpdateFilter(update_filter);
}

template <typename TrackerIdType>
void Transaction::overrideTrackerIdentificationMethod(
    NetTable* trackee_table, NetTable* tracker_table,
    const std::function<TrackerIdType(const Revision&)>&
        how_to_determine_tracker) {
  CHECK_NOTNULL(trackee_table);
  CHECK_NOTNULL(tracker_table);
  CHECK(how_to_determine_tracker);
  CHECK(!finalized_);
  enableDirectAccess();
  transactionOf(trackee_table)->overrideTrackerIdentificationMethod(
      tracker_table, how_to_determine_tracker);
  disableDirectAccess();
}

template <typename IdType>
std::shared_ptr<const Revision>* Transaction::getMutableUpdateEntry(
    const IdType& id, NetTable* table) {
  CHECK(!finalized_);
  return transactionOf(CHECK_NOTNULL(table))->getMutableUpdateEntry(id);
}

template <typename IdType, typename ObjectType>
ThreadsafeCache<IdType, ObjectType>* Transaction::getMutableCache(
    NetTable* table) {
  CHECK_NOTNULL(table);
  CHECK(!finalized_);
  // This ABSOLUTELY MUST REMAIN A DYNAMIC_CAST!!! Previously, it was static,
  // and resulted in a 3-day bug hunt.
  ThreadsafeCache<IdType, ObjectType>* result =
      dynamic_cast<ThreadsafeCache<IdType, ObjectType>*>(  // NOLINT
          dmap_common::getChecked(caches_, table).get());
  CHECK(result != nullptr) << "Requested cache type does not correspond to "
                           << "cache type previously created for table "
                           << table->name() << "!";
  return result;
}

}  // namespace dmap

#endif  // DMAP_TRANSACTION_INL_H_
