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

#ifndef DMAP_CHUNK_TRANSACTION_INL_H_
#define DMAP_CHUNK_TRANSACTION_INL_H_

#include <string>
#include <utility>
#include <vector>

#include <map-api-common/unique-id.h>

namespace map_api {

template <typename ValueType>
void ChunkTransaction::addConflictCondition(int key, const ValueType& value) {
  CHECK(!finalized_);
  std::shared_ptr<Revision> value_holder =
      chunk_->data_container_->getTemplate();
  value_holder->set(key, value);
  conflict_conditions_.push_back(ConflictCondition(key, value_holder));
}

template <typename IdType>
std::shared_ptr<const Revision> ChunkTransaction::getById(const IdType& id)
    const {
  return combined_view_.get(id.template toIdType<map_api_common::Id>());
}

template <typename IdType>
void ChunkTransaction::getAvailableIds(std::unordered_set<IdType>* ids) const {
  CHECK_NOTNULL(ids)->clear();
  std::unordered_set<map_api_common::Id> common_ids;
  combined_view_.getAvailableIds(&common_ids);
  for (const map_api_common::Id& id : common_ids) {
    ids->emplace(id.template toIdType<IdType>());
  }
}

template <typename IdType>
void ChunkTransaction::getMutableUpdateEntry(
    const IdType& id, std::shared_ptr<const Revision>** result) {
  CHECK(!finalized_);
  CHECK_NOTNULL(result);
  map_api_common::Id common_id = id.template toIdType<map_api_common::Id>();
  if (!delta_.getMutableUpdateEntry(common_id, result)) {
    std::shared_ptr<const Revision> original = getById(id);
    CHECK(original);
    std::shared_ptr<Revision> to_emplace;
    original->copyForWrite(&to_emplace);
    update(to_emplace);
    CHECK(delta_.getMutableUpdateEntry(common_id, result));
  }
}

template <typename ValueType>
std::shared_ptr<const Revision> ChunkTransaction::findUnique(
    int key, const ValueType& value) const {
  // FIXME(tcies) Also search in uncommitted.
  // FIXME(tcies) Also search in previously committed.
  std::shared_ptr<const Revision> result =
      chunk_->constData()->findUnique(key, value, begin_time_);
  return result;
}

}  // namespace map_api

#endif  // DMAP_CHUNK_TRANSACTION_INL_H_
