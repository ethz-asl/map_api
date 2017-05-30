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

#ifndef MAP_API_STXXL_REVISION_STORE_H_
#define MAP_API_STXXL_REVISION_STORE_H_

#include <memory>
#include <mutex>
#include <vector>

#include <stxxl.h>

#include "map-api/proto-stl-stream.h"
#include "map-api/revision.h"

namespace map_api {
struct CRRevisionInformation {
  MemoryBlockInformation memory_block_;
  // Cache information which is frequently accessed.
  virtual void SetFromRevision(const Revision& revision) {
    insert_time_ = revision.getInsertTime();
    chunk_id_ = revision.getChunkId();
  }
  LogicalTime insert_time_;
  map_api_common::Id chunk_id_;
};
struct CRURevisionInformation : public CRRevisionInformation {
  // Cache information which is frequently accessed.
  virtual void SetFromRevision(const Revision& revision) {
    CRRevisionInformation::SetFromRevision(revision);
    update_time_ = revision.getModificationTime();
    is_removed_ = revision.isRemoved();
  }
  LogicalTime update_time_;
  bool is_removed_;
};

static constexpr int kSTXXLDefaultBlockSize = 128;

template <int BlockSize>
class STXXLRevisionStore {
 public:
  inline bool storeRevision(const Revision& revision,
                            CRRevisionInformation* revision_info) {
    CHECK_NOTNULL(revision_info);

    std::unique_lock<std::mutex> lock(mutex_);
    revision_info->SetFromRevision(revision);
    STLContainerOutputStream<BlockSize, ContainerType> output_stream(
        &proto_revision_pool_);

    MemoryBlockInformation& block_information = revision_info->memory_block_;
    bool status = output_stream.WriteMessage(*revision.underlying_revision_,
                                             &block_information);
    return status;
  }

  inline bool retrieveRevision(const CRRevisionInformation& revision_info,
                               std::shared_ptr<const Revision>* revision)
      const {
    CHECK_NOTNULL(revision);
    std::unique_lock<std::mutex> lock(mutex_);
    const MemoryBlockInformation& block_information =
        revision_info.memory_block_;

    STLContainerInputStream<BlockSize, ContainerType> input_stream(
        block_information.block_index, block_information.byte_offset,
        &proto_revision_pool_);

    std::unique_ptr<proto::Revision> proto_in(new proto::Revision);
    bool status = input_stream.ReadMessage(proto_in.get());
    Revision::fromProto(std::move(proto_in), revision);

    CHECK_EQ(revision_info.insert_time_, (*revision)->getInsertTime());
    return status;
  }

 private:
  template <typename ValueType, unsigned PageSize = 2, unsigned CachePages = 4,
            unsigned BlockSizeStxxl = 1024 * 1024,
            typename AllocStr = STXXL_DEFAULT_ALLOC_STRATEGY,
            stxxl::pager_type Pager = stxxl::lru>
  struct VectorGenerator {
    typedef typename stxxl::IF<
        Pager == stxxl::lru, stxxl::lru_pager<CachePages>,
        stxxl::random_pager<CachePages> >::result PagerType;

    typedef stxxl::vector<ValueType, PageSize, PagerType, BlockSizeStxxl,
                          AllocStr> result;
  };

  template <class T, class A>
  using ContainerType = typename VectorGenerator<T>::result;
  mutable MemoryBlockPool<BlockSize, ContainerType> proto_revision_pool_;
  mutable std::mutex mutex_;
};
}  // namespace map_api
#endif  // MAP_API_STXXL_REVISION_STORE_H_
