#ifndef MAP_API_OBJECT_CACHE_H_
#define MAP_API_OBJECT_CACHE_H_

#include <string>
#include <vector>

#include <gtest/gtest_prod.h>
#include <multiagent-mapping-common/mapped-container-base.h>

#include "map-api/cache-base.h"
#include "map-api/internal/object-and-metadata-cache.h"
#include "map-api/net-table.h"
#include "map-api/transaction.h"

namespace map_api {

// This is a threadsafe MappedContainerBase implementation intended for use by
// Map API applications. It can be obtained using Transaction::createCache().
template <typename IdType, typename ObjectType>
class ObjectCache : public common::MappedContainerBase<IdType, ObjectType>,
                    public CacheBase {
 public:
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
    if (table_->name() == "visual_inertial_mapping_mission_table") {
      VLOG(5) << "getMutable() on " << id;
    }
    return cache_.getMutable(id).object;
  }

  virtual const ObjectType& get(const IdType& id) const {
    CHECK(cache_.get(id).metadata);
    return cache_.get(id).object;
  }

  virtual bool insert(const IdType& id, const ObjectType& value) {
    ObjectAndMetadata<ObjectType> insertion;
    insertion.createForInsert(value, table_);
    return cache_.insert(id, insertion);
  }

  virtual void erase(const IdType& id) { cache_.erase(id); }

  // ====================
  // CACHE BASE INTERFACE
  // ====================
  virtual std::string underlyingTableName() const { return table_->name(); }

  virtual void prepareForCommit() {
    VLOG(3) << "Flushing object cache for table " << table_->name() << "...";
    cache_.flush();
  }

  virtual size_t numCachedItems() const {
    LOG(FATAL) << "Not supported atm.";
    return 0u;
  }

  // =============
  // OWN FUNCTIONS
  // =============
  void getTrackedChunks(const IdType& id, TrackeeMultimap* result) const {
    const ObjectAndMetadata<ObjectType>& object_metadata = cache_.get(id);
    CHECK(object_metadata.metadata);
    object_metadata.metadata->getTrackedChunks(CHECK_NOTNULL(result));
  }

 private:
  ObjectCache(Transaction* const transaction, NetTable* const table)
      : table_(CHECK_NOTNULL(table)),
        chunk_manager_(kDefaultChunkSizeBytes, table),
        transaction_interface_(CHECK_NOTNULL(transaction), table,
                               &chunk_manager_),
        cache_(&transaction_interface_) {}
  friend class Transaction;

  NetTable* const table_;
  ChunkManagerChunkSize chunk_manager_;
  template <typename T>
  friend class CacheAndTransactionTest;
  NetTableTransactionInterface<IdType> transaction_interface_;
  ObjectAndMetadataCache<IdType, ObjectType> cache_;
};

}  // namespace map_api

#endif  // MAP_API_OBJECT_CACHE_H_
