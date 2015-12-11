#ifndef MAP_API_THREADSAFE_CACHE_H_
#define MAP_API_THREADSAFE_CACHE_H_

#include <string>
#include <vector>

#include <gtest/gtest_prod.h>
#include <multiagent-mapping-common/mapped-container-base.h>
#include <multiagent-mapping-common/monitor.h>

#include "map-api/cache-base.h"
#include "map-api/internal/threadsafe-object-and-metadata-cache.h"
#include "map-api/net-table.h"
#include "map-api/transaction.h"

namespace map_api {

// This is a threadsafe MappedContainerBase implementation intended for use by
// Map API applications. It can be obtained using Transaction::createCache().
template <typename IdType, typename ObjectType>
class ThreadsafeCache : public common::MappedContainerBase<IdType, ObjectType>,
                        public CacheBase {
 public:
  typedef common::MappedContainerBase<IdType, ObjectType> Base;
  // ==========================
  // MAPPED CONTAINER INTERFACE
  // ==========================
  virtual bool has(const IdType& id) const { return cache_.has(id); }

  virtual void getAllAvailableIds(std::vector<IdType>* available_ids) const {
    cache_.getAllAvailableIds(available_ids);
  }
  virtual size_t size() const { return cache_.size(); }

  virtual bool empty() const { return cache_.empty(); }

  virtual ObjectType& getMutable(const IdType& id) {
    return cache_.getMutable(id).object;
  }

  virtual typename Base::ConstRefReturnType get(const IdType& id) const {
    CHECK(cache_.get(id).metadata);
    return cache_.get(id).object;
  }

  virtual bool insert(const IdType& id, const ObjectType& value) {
    ObjectAndMetadata<ObjectType> insertion;
    insertion.createForInsert(value, table_);
    insertions_.get()->emplace(id);
    return cache_.insert(id, insertion);
  }

  virtual void erase(const IdType& id) {
    cache_.erase(id);
    typename common::Monitor<std::unordered_set<IdType>>::ThreadSafeAccess&&
        insertions = insertions_.get();
    typename std::unordered_set<IdType>::iterator found = insertions->find(id);
    if (found != insertions->end()) {
      insertions->erase(found);
    }
  }

  // ====================
  // CACHE BASE INTERFACE
  // ====================
  virtual std::string underlyingTableName() const { return table_->name(); }

  virtual void prepareForCommit() {
    VLOG(3) << "Flushing object cache for table " << table_->name() << "...";
    cache_.flush();
  }

  virtual void discardCachedInsertions() {
    typename common::Monitor<std::unordered_set<IdType>>::ThreadSafeAccess&&
        insertions = insertions_.get();
    for (const IdType& id : *insertions) {
      cache_.discardCached(id);
    }
    insertions->clear();
  }

  virtual void refreshAvailableIds() { cache_.refreshAvailableIds(); }

  // =============
  // OWN FUNCTIONS
  // =============
  void getTrackedChunks(const IdType& id, TrackeeMultimap* result) const {
    const ObjectAndMetadata<ObjectType>& object_metadata = cache_.get(id);
    CHECK(object_metadata.metadata);
    object_metadata.metadata->getTrackedChunks(CHECK_NOTNULL(result));
  }

  // Add a function to determine whether updates should be applied back to the
  // cache (true = will be applied).
  // Attention, this will be very expensive, since it will add two conversions
  // per item! Prefer to use const correctness if possible.
  void setUpdateFilter(
      const std::function<bool(const ObjectType& original,  // NOLINT
                               const ObjectType& innovation)>& update_filter) {
    CHECK(update_filter);
    cache_.setUpdateFilter([&update_filter](
        const std::shared_ptr<const Revision>& original_revision,
        const std::shared_ptr<const Revision>& innovation_revision) {
      CHECK(original_revision);
      CHECK(innovation_revision);
      ObjectType original, innovation;
      objectFromRevision(*original_revision, &original);
      objectFromRevision(*innovation_revision, &innovation);
      return update_filter(original, innovation);
    });
  }

 private:
  ThreadsafeCache(Transaction* const transaction, NetTable* const table)
      : table_(CHECK_NOTNULL(table)),
        chunk_manager_(kDefaultChunkSizeBytes, table),
        transaction_interface_(CHECK_NOTNULL(transaction), table,
                               &chunk_manager_),
        cache_(&transaction_interface_),
        insertions_(std::unordered_set<IdType>()) {}

  template <typename T>
  friend class CacheAndTransactionTest;
  friend class Transaction;

  NetTable* const table_;
  ChunkManagerChunkSize chunk_manager_;
  NetTableTransactionInterface<IdType> transaction_interface_;
  ThreadsafeObjectAndMetadataCache<IdType, ObjectType> cache_;
  common::Monitor<std::unordered_set<IdType>> insertions_;
};

}  // namespace map_api

#endif  // MAP_API_THREADSAFE_CACHE_H_
