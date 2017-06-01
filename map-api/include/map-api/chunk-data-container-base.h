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

#ifndef MAP_API_CHUNK_DATA_CONTAINER_BASE_H_
#define MAP_API_CHUNK_DATA_CONTAINER_BASE_H_

#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include "map-api/table-descriptor.h"
#include "./core.pb.h"

namespace map_api_common {
class Id;
}  // namespace common

namespace map_api {
class LegacyChunk;
class ConstRevisionMap;
class MutableRevisionMap;
class Revision;

class ChunkDataContainerBase {
  friend class LegacyChunk;

 public:
  virtual ~ChunkDataContainerBase();
  ChunkDataContainerBase();

  bool init(std::shared_ptr<TableDescriptor> descriptor);
  bool isInitialized() const;
  const std::string& name() const;
  std::shared_ptr<Revision> getTemplate() const;

  // ====
  // READ
  // ====
  /**
   * Returns revision of item that has been current at "time" or an invalid
   * pointer if the item hasn't been inserted at "time"
   */
  template <typename IdType>
  std::shared_ptr<const Revision> getById(const IdType& id,
                                          const LogicalTime& time) const;
  // If "key" is -1, no filter will be applied
  template <typename ValueType>
  void find(int key, const ValueType& value, const LogicalTime& time,
            ConstRevisionMap* dest) const;
  void dump(const LogicalTime& time, ConstRevisionMap* dest) const;
  template <typename ValueType>
  std::shared_ptr<const Revision> findUnique(int key, const ValueType& value,
                                             const LogicalTime& time) const;
  void findByRevision(int key, const Revision& valueHolder,
                      const LogicalTime& time, ConstRevisionMap* dest) const;

  // ====
  // MISC
  // ====
  template <typename IdType>
  void getAvailableIds(const LogicalTime& time, std::vector<IdType>* ids) const;
  int numAvailableIds(const LogicalTime& time) const;
  // If "key" is -1, no filter will be applied.
  template <typename ValueType>
  int count(int key, const ValueType& value, const LogicalTime& time) const;
  virtual int countByRevision(int key, const Revision& valueHolder,
                              const LogicalTime& time) const final;
  bool getLatestUpdateTime(const map_api_common::Id& id, LogicalTime* time);
  struct ItemDebugInfo {
    std::string table;
    std::string id;
    ItemDebugInfo(const std::string& _table, const map_api_common::Id& _id)
        : table(_table), id(_id.hexString()) {}
  };

 protected:
  mutable std::mutex access_mutex_;
  std::shared_ptr<TableDescriptor> descriptor_;

 private:
  /**
   * ================================================
   * FUNCTIONS TO BE IMPLEMENTED BY THE DERIVED CLASS
   * ================================================
   */
  // Do here whatever is specific to initializing the derived type
  virtual bool initImpl() = 0;
  virtual std::shared_ptr<const Revision> getByIdImpl(
      const map_api_common::Id& id, const LogicalTime& time) const = 0;
  // If key is -1, this should return all the data in the table.
  virtual void findByRevisionImpl(int key, const Revision& valueHolder,
                                  const LogicalTime& time,
                                  ConstRevisionMap* dest) const = 0;
  virtual void getAvailableIdsImpl(const LogicalTime& time,
                                   std::vector<map_api_common::Id>* ids) const = 0;
  // If key is -1, this should return all the data in the table.
  virtual int countByRevisionImpl(int key, const Revision& valueHolder,
                                  const LogicalTime& time) const = 0;

  bool initialized_;
};

std::ostream& operator<<(std::ostream& stream,
                         const ChunkDataContainerBase::ItemDebugInfo& info);

}  // namespace map_api

#include "./chunk-data-container-base-inl.h"

#endif  // MAP_API_CHUNK_DATA_CONTAINER_BASE_H_
