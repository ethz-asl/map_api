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
// along with Map API. If not, see <http://www.gnu.org/licenses/>.

#ifndef INTERNAL_THREADSAFE_OBJECT_AND_METADATA_CACHE_H_
#define INTERNAL_THREADSAFE_OBJECT_AND_METADATA_CACHE_H_

#include <string>

#include <map-api-common/threadsafe-cache.h>

#include "map-api/cache-base.h"
#include "map-api/internal/object-and-metadata.h"
#include "map-api/net-table-transaction-interface.h"

namespace map_api {

template <typename IdType, typename ObjectType>
class ThreadsafeObjectAndMetadataCache
    : public map_api_common::ThreadsafeCache<IdType, std::shared_ptr<const Revision>,
                                     ObjectAndMetadata<ObjectType>> {
 public:
  typedef map_api_common::ThreadsafeCache<IdType, std::shared_ptr<const Revision>,
                                  ObjectAndMetadata<ObjectType>> BaseType;

  virtual ~ThreadsafeObjectAndMetadataCache() {}

 private:
  // Takes ownership of the interface.
  explicit ThreadsafeObjectAndMetadataCache(
      NetTableTransactionInterface<IdType>* interface)
      : BaseType(CHECK_NOTNULL(interface)) {}
  friend class ThreadsafeCache<IdType, ObjectType>;

  virtual void rawToCacheImpl(const std::shared_ptr<const Revision>& raw,
                              ObjectAndMetadata<ObjectType>* cached) const
      final override {
    CHECK(raw);
    CHECK_NOTNULL(cached);
    cached->deserialize(*raw);
    CHECK(cached->metadata);
  }

  virtual void cacheToRawImpl(const ObjectAndMetadata<ObjectType>& cached,
                              std::shared_ptr<const Revision>* raw) const
      final override {
    CHECK_NOTNULL(raw);
    cached.serialize(raw);
  }

  virtual bool shouldUpdateImpl(const std::shared_ptr<const Revision>& original,
                                const std::shared_ptr<const Revision>& updated)
      const {
    CHECK(original);
    CHECK(updated);
    return !original->areAllCustomFieldsEqual(*updated);
  }
};

}  // namespace map_api

#endif  // INTERNAL_THREADSAFE_OBJECT_AND_METADATA_CACHE_H_
