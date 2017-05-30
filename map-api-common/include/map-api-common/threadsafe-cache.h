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

#ifndef MAP_API_COMMON_THREADSAFE_CACHE_H_
#define MAP_API_COMMON_THREADSAFE_CACHE_H_

#include <iostream>  // NOLINT
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "map-api-common/backtrace.h"
#include "map-api-common/mapped-container-base.h"

DECLARE_bool(cache_blame_dirty);
DECLARE_uint64(cache_blame_dirty_sampling);
DECLARE_bool(cache_blame_insert);

namespace map_api_common {

// If looking up a resource is costly, this class can be used to cache it in a
// hash map. In order to avoid unnecessarily redeclaring pure virtual functions,
// it is assumed that the Raw data is available as a MappedContainerBase itself.
// If this is not the case, you need to write a MappedContainerBase interface
// to the raw resource.
template <typename IdType, typename RawType, typename CachedType = RawType>
class ThreadsafeCache : public MappedContainerBase<IdType, CachedType> {
 public:
  explicit ThreadsafeCache(
      MappedContainerBase<IdType, RawType>* raw_container)
      : raw_container_(CHECK_NOTNULL(raw_container)) {
    refreshAvailableIds();
  }

  virtual ~ThreadsafeCache() {}

  // ===============================
  // MAPPED CONTAINER BASE FUNCTIONS
  // ===============================
  virtual bool has(const IdType& id) const final override {
    std::lock_guard<std::mutex> lock(m_available_ids_);
    return available_ids_.count(id) > 0u;
  }

  virtual void getAllAvailableIds(std::vector<IdType>* available_ids) const
      final override {
    CHECK_NOTNULL(available_ids)->clear();
    std::lock_guard<std::mutex> lock(m_available_ids_);
    available_ids->insert(available_ids->end(), available_ids_.begin(),
                          available_ids_.end());
  }

  virtual size_t size() const {
    std::lock_guard<std::mutex> lock(m_available_ids_);
    return available_ids_.size();
  }

  virtual bool empty() const final override {
    std::lock_guard<std::mutex> lock(m_available_ids_);
    return available_ids_.empty();
  }

  virtual CachedType& getMutable(const IdType& id) final override {
    std::lock_guard<std::mutex> lock(m_cache_and_raw_);
    CacheStruct* cached = getImplLocked(id);
    CHECK_NOTNULL(cached);
    cached->dirty = true;
    if (FLAGS_cache_blame_dirty) {
      static size_t i = 0u;
      if (i % FLAGS_cache_blame_dirty_sampling == 0u) {
        ++unique_dirty_backtraces_[backtrace()];
      }
      ++i;
    }
    return cached->value;
  }

  virtual const CachedType& get(const IdType& id) const final override {
    std::lock_guard<std::mutex> lock(m_cache_and_raw_);
    const CacheStruct* cached = getImplLocked(id);
    CHECK_NOTNULL(cached);
    CHECK(!cached->to_remove);
    return cached->value;
  }

  virtual bool insert(const IdType& id,
                      const CachedType& value) final override {
    // Follows lock ordering.
    std::lock_guard<std::mutex> id_lock(m_available_ids_);
    std::lock_guard<std::mutex> lock(m_cache_and_raw_);
    std::pair<typename Cache::iterator, bool> emplace_result =
        cache_.emplace(id, std::unique_ptr<CacheStruct>(new CacheStruct));
    if (!emplace_result.second) {
      return false;
    }
    emplace_result.first->second->value = value;
    emplace_result.first->second->dirty = false;
    emplace_result.first->second->to_remove = false;
    available_ids_.emplace(id);
    if (FLAGS_cache_blame_insert) {
      ++unique_insert_backtraces_[backtrace()];
    }
    return true;
  }

  virtual void erase(const IdType& id) final override {
    // Pre-lock necessary for lock ordering.
    std::lock_guard<std::mutex> id_lock(m_available_ids_);
    std::lock_guard<std::mutex> lock(m_cache_and_raw_);
    CacheStruct* cached = getImplLocked(id);
    CHECK_NOTNULL(cached);
    cached->to_remove = true;
    available_ids_.erase(id);
  }

  // ===================================
  // THREADSAFE-CACHE-SPECIFIC FUNCTIONS
  // ===================================
  // Apply cache state to raw state.
  void flush() {
    std::lock_guard<std::mutex> lock(m_cache_and_raw_);

    std::vector<IdType> raw_ids_vector;
    raw_container_->getAllAvailableIds(&raw_ids_vector);
    const IdSet raw_ids_set(raw_ids_vector.begin(), raw_ids_vector.end());

    size_t num_insertions = 0u;
    size_t num_updates = 0u;
    size_t num_removals = 0u;
    for (const typename Cache::value_type& cache_pair : cache_) {
      if (!cache_pair.second->to_remove) {
        // An id not existing in the raw id set implies that the corresponding
        // item doesn't exist in the raw container, thus we can assume that it's
        // a new insertion.
        if (raw_ids_set.count(cache_pair.first) == 0u) {
          // Insertion.
          RawType to_insert;
          cacheToRawImpl(cache_pair.second->value, &to_insert);
          raw_container_->insert(cache_pair.first, to_insert);
          ++num_insertions;
        } else {
          if (cache_pair.second->dirty) {
            // Update.
            RawType to_update;
            cacheToRawImpl(cache_pair.second->value, &to_update);
            // Not using a virtual function for update filtering on purpose.
            // Not only is this probably faster to execute, but it also doesn't
            // require a derived class for each object type.
            if (update_filter_ &&
                !update_filter_(raw_container_->get(cache_pair.first),
                                to_update)) {
              continue;
            }
            raw_container_->getMutable(cache_pair.first) = to_update;
            ++num_updates;
          }
        }
      } else {  // To remove.
        if (raw_ids_set.count(cache_pair.first) != 0u) {
          // Removal.
          raw_container_->erase(cache_pair.first);
          ++num_removals;
        }
      }
    }

    VLOG(4) << "Flush: Insertions: " << num_insertions
            << " updates: " << num_updates << " removals: " << num_removals;

    if (FLAGS_cache_blame_dirty) {
      std::cout << "This cache has been made dirty from "
                << unique_dirty_backtraces_.size()
                << " locations:" << std::endl;
      for (const BacktraceMap::value_type& trace : unique_dirty_backtraces_) {
        std::cout << std::endl << trace.second << " times from:" << std::endl;
        std::cout << trace.first << std::endl;
      }
      // Reset the counters to better understand what is happening when with
      // multiple flushes.
      unique_dirty_backtraces_.clear();
    }
    if (FLAGS_cache_blame_insert) {
      std::cout << "This cache has been inserted to from "
                << unique_insert_backtraces_.size()
                << " locations:" << std::endl;
      for (const BacktraceMap::value_type& trace : unique_insert_backtraces_) {
        std::cout << std::endl << trace.second << " times from:" << std::endl;
        std::cout << trace.first << std::endl;
      }
      unique_insert_backtraces_.clear();
    }

    // Refresh cache state: Nothing is dirty any more, removed items are
    // removed.
    for (typename Cache::iterator it = cache_.begin(); it != cache_.end();) {
      if (it->second->to_remove) {
        it = cache_.erase(it);
      } else {
        ++it;
      }
    }
    for (typename Cache::value_type& cache_pair : cache_) {
      cache_pair.second->dirty = false;
    }
  }

  void discardCached(IdType id) { CHECK_EQ(cache_.erase(id), 1u); }

  // If new ids are supposed to be available in the raw container.
  void refreshAvailableIds() {
    std::lock_guard<std::mutex> id_lock(m_available_ids_);
    available_ids_.clear();
    std::vector<IdType> raw_ids;
    raw_container_->getAllAvailableIds(&raw_ids);
    available_ids_.reserve(raw_ids.size());
    available_ids_.insert(raw_ids.begin(), raw_ids.end());
  }

  // Add a function to determine whether updates should be applied back to the
  // cache (true = will be applied).
  void setUpdateFilter(const std::function<bool(
      const RawType& original, const RawType& innovation)>& update_filter) {
    CHECK(update_filter);
    CHECK(!update_filter_) << "Tried to overwrite update filter!";
    update_filter_ = update_filter;
  }

 private:
  struct CacheStruct {
    CachedType value;
    bool dirty;
    bool to_remove;
  };

  // Storing items through pointers ensures that they can be passed as reference
  // even if the map is volatile.
  typedef std::unordered_map<IdType, std::unique_ptr<CacheStruct>> Cache;
  typedef MappedContainerBase<IdType, RawType>* RawContainerPtr;
  typedef std::unordered_set<IdType> IdSet;

  CacheStruct* getImplLocked(const IdType& id) const {
    typename Cache::iterator found = cache_.find(id);
    if (found == cache_.end()) {
      // Not bothering with const_ref_type hacks right now.
      const RawType& raw = raw_container_->get(id);
      cache_[id].reset(new CacheStruct);
      CacheStruct* result = cache_[id].get();
      rawToCacheImpl(raw, &result->value);
      result->dirty = false;
      result->to_remove = false;
      return result;
    }
    CHECK(found->second);
    CHECK(!found->second->to_remove);
    return found->second.get();
  }

  mutable Cache cache_;
  RawContainerPtr const raw_container_;
  IdSet available_ids_;

  typedef std::unordered_map<std::string, size_t> BacktraceMap;
  BacktraceMap unique_dirty_backtraces_;
  BacktraceMap unique_insert_backtraces_;

  // Lock ordering in order of declaration:
  mutable std::mutex m_available_ids_;
  // Reader-writer lock could be considered for cache_and_raw_.
  mutable std::mutex m_cache_and_raw_;

  virtual void rawToCacheImpl(const RawType& raw, CachedType* cached) const = 0;
  virtual void cacheToRawImpl(const CachedType& cached, RawType* raw) const = 0;

  std::function<
      bool(const RawType& original, const RawType& innovation)>  // NOLINT
      update_filter_;
};

}  // namespace map_api_common

#endif  // MAP_API_COMMON_THREADSAFE_CACHE_H_
