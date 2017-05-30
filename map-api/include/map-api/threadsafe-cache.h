// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

#ifndef MAP_API_THREADSAFE_CACHE_H_
#define MAP_API_THREADSAFE_CACHE_H_

#include <string>
#include <vector>

#include <gtest/gtest_prod.h>
#include <map-api-common/mapped-container-base.h>
#include <map-api-common/monitor.h>

#include "map-api/cache-base.h"
#include "map-api/internal/threadsafe-object-and-metadata-cache.h"
#include "map-api/net-table.h"
#include "map-api/transaction.h"

namespace map_api {

// This is a threadsafe MappedContainerBase implementation intended for use by
// Map API applications. It can be obtained using Transaction::createCache().
template <typename IdType, typename ObjectType>
class ThreadsafeCache : public map_api_common::MappedContainerBase<IdType, ObjectType>,
                        public CacheBase {
 public:
  typedef map_api_common::MappedContainerBase<IdType, ObjectType> Base;
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
    const ObjectAndMetadata<ObjectType>& cached = cache_.get(id);
    CHECK(cached.metadata);
    return cached.object;
  }

  virtual bool insert(const IdType& id, const ObjectType& value) {
    ObjectAndMetadata<ObjectType> insertion;
    insertion.createForInsert(value, table_);
    insertions_.get()->emplace(id);
    return cache_.insert(id, insertion);
  }

  virtual void erase(const IdType& id) {
    cache_.erase(id);
    typename map_api_common::Monitor<std::unordered_set<IdType>>::ThreadSafeAccess&&
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
    typename map_api_common::Monitor<std::unordered_set<IdType>>::ThreadSafeAccess&&
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
  map_api_common::Monitor<std::unordered_set<IdType>> insertions_;
};

}  // namespace map_api

#endif  // MAP_API_THREADSAFE_CACHE_H_
