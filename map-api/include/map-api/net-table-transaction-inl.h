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

#ifndef MAP_API_NET_TABLE_TRANSACTION_INL_H_
#define MAP_API_NET_TABLE_TRANSACTION_INL_H_

#include <string>
#include <vector>

namespace map_api {

template <typename IdType>
std::shared_ptr<const Revision> NetTableTransaction::getById(const IdType& id)
    const {
  ChunkBase* chunk = chunkOf(id);
  if (chunk == nullptr) {
    return std::shared_ptr<Revision>();
  }
  return getById(id, chunk);
}

template <typename IdType>
std::shared_ptr<const Revision> NetTableTransaction::getById(
    const IdType& id, ChunkBase* chunk) const {
  CHECK_NOTNULL(chunk);
  if (!workspace_.contains(chunk->id())) {
    return std::shared_ptr<Revision>();
  }
  return transactionOf(chunk)->getById(id);
}

template <typename ValueType>
void NetTableTransaction::find(int key, const ValueType& value,
                               ConstRevisionMap* result) {
  CHECK_NOTNULL(result);
  // TODO(tcies) Also search in uncommitted.
  // TODO(tcies) Also search in previously committed.
  workspace_.forEachChunk([&, this](const ChunkBase& chunk) {
    ConstRevisionMap chunk_result;
    chunk.constData()->find(key, value, begin_time_, &chunk_result);
    result->insert(chunk_result.begin(), chunk_result.end());
  });
}

template <typename IdType>
void NetTableTransaction::getAvailableIds(std::vector<IdType>* ids) {
  CHECK_NOTNULL(ids)->clear();
  ids->reserve(item_id_to_chunk_id_map_.size());
  for (const ItemIdToChunkIdMap::value_type& item_chunk_ids :
       item_id_to_chunk_id_map_) {
    ids->push_back(item_chunk_ids.first.toIdType<IdType>());
  }
}

template <typename IdType>
std::shared_ptr<const Revision>* NetTableTransaction::getMutableUpdateEntry(
    const IdType& id) {
  CHECK(!finalized_);
  ChunkBase* chunk = chunkOf(id);
  CHECK_NOTNULL(chunk);
  CHECK(workspace_.contains(chunk->id()));
  std::shared_ptr<const Revision>* result;
  transactionOf(chunk)->getMutableUpdateEntry(id, &result);
  return result;
}

template <typename IdType>
void NetTableTransaction::remove(const IdType& id) {
  CHECK(!finalized_);
  ChunkBase* chunk = chunkOf(id);
  CHECK_NOTNULL(chunk);
  std::shared_ptr<Revision> remove_revision;
  getById(id, chunk)->copyForWrite(&remove_revision);
  transactionOf(chunk)->remove(remove_revision);
}

template <typename IdType>
ChunkBase* NetTableTransaction::chunkOf(const IdType& id) const {
  map_api_common::Id common_id;
  id.toHashId(&common_id);
  ItemIdToChunkIdMap::const_iterator found =
      item_id_to_chunk_id_map_.find(common_id);
  if (found == item_id_to_chunk_id_map_.end()) {
    return nullptr;
  } else {
    return table_->getChunk(found->second);
  }
}

template <typename TrackerIdType>
void NetTableTransaction::overrideTrackerIdentificationMethod(
    NetTable* tracker_table,
    const std::function<TrackerIdType(const Revision&)>&
        how_to_determine_tracker) {
  CHECK_NOTNULL(tracker_table);
  CHECK(how_to_determine_tracker);
  CHECK(!finalized_);
  CHECK_GT(table_->new_chunk_trackers().count(tracker_table), 0u)
      << "Attempted to override a tracker identification method which is "
      << "however not used for pushing new chunk ids.";
  auto determine_map_api_tracker_id = [how_to_determine_tracker](
      const Revision& trackee) {
    return static_cast<map_api_common::Id>(how_to_determine_tracker(trackee));
  };

  push_new_chunk_ids_to_tracker_overrides_[tracker_table] =
      determine_map_api_tracker_id;
}

}  // namespace map_api

#endif  // MAP_API_NET_TABLE_TRANSACTION_INL_H_
