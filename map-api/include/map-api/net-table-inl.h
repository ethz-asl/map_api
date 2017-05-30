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

#ifndef MAP_API_NET_TABLE_INL_H_
#define MAP_API_NET_TABLE_INL_H_

#include <vector>

namespace map_api {

/**
 * Making this deprecated because I don't really like it...
 */
template <typename IdType>
void __attribute__((deprecated)) NetTable::registerItemInSpace(
    const IdType& id, const SpatialIndex::BoundingBox& bounding_box) {
  std::shared_ptr<const Revision> item_revision =
      getById(id, LogicalTime::sample());
  registerChunkInSpace(item_revision->getChunkId(), bounding_box);
}

template <typename ValueType>
void NetTable::lockFind(int key, const ValueType& value,
                        const LogicalTime& time,
                        ConstRevisionMap* result) const {
  CHECK_NOTNULL(result)->clear();
  forEachActiveChunk([&](const ChunkBase& chunk) {
    ConstRevisionMap chunk_result;
    chunk.constData()->find(key, value, time, &chunk_result);
    result->insert(chunk_result.begin(), chunk_result.end());
  });
}

template <typename IdType>
std::shared_ptr<const Revision> NetTable::getById(const IdType& id,
                                                  const LogicalTime& time) {
  std::shared_ptr<const Revision> result;
  forEachActiveChunkUntil([&](const ChunkBase& chunk) {
    result = chunk.constData()->getById(id, time);
    return static_cast<bool>(result);
  });
  return result;
}

template <typename IdType>
void NetTable::getAvailableIds(const LogicalTime& time,
                               std::vector<IdType>* ids) {
  CHECK_NOTNULL(ids)->clear();
  forEachActiveChunk([&](const ChunkBase& chunk) {
    std::vector<IdType> chunk_result;
    chunk.constData()->getAvailableIds(time, &chunk_result);
    ids->insert(ids->end(), chunk_result.begin(), chunk_result.end());
  });
}

template <typename TrackeeType, typename TrackerType, typename TrackerIdType>
std::function<map_api_common::Id(const Revision&)>
NetTable::trackerDeterminerFactory() {
  return [](const Revision& trackee_revision) {  // NOLINT
    std::shared_ptr<TrackeeType> trackee;
    objectFromRevision(trackee_revision, &trackee);
    TrackerIdType typed_tracker_id =
        determineTracker<TrackeeType, TrackerType, TrackerIdType>(*trackee);
    return static_cast<map_api_common::Id>(typed_tracker_id);
  };
}

template <typename TrackeeType, typename TrackerType, typename TrackerIdType>
void NetTable::pushNewChunkIdsToTracker() {
  NetTable* tracker_table = tableForType<TrackerType>();
  this->pushNewChunkIdsToTracker(
      tracker_table,
      trackerDeterminerFactory<TrackeeType, TrackerType, TrackerIdType>());
}

template <>
void NetTable::followTrackedChunksOfItem(const map_api_common::Id& item_id,
                                         ChunkBase* tracker_chunk);

template <typename IdType>
void NetTable::followTrackedChunksOfItem(const IdType& item_id,
                                         ChunkBase* tracker_chunk) {
  map_api_common::Id common_id;
  common_id.fromHashId(item_id.toHashId());
  followTrackedChunksOfItem(item_id, tracker_chunk);
}

template <typename ObjectType>
void NetTable::addAutoMergePolicy(
    const typename AutoMergePolicy<ObjectType>::Type& auto_merge_policy) {
  addAutoMergePolicy([auto_merge_policy](const Revision& conflicting_revision,
                                         const Revision& original_revision,
                                         Revision* revision_at_hand) {
    CHECK_NOTNULL(revision_at_hand);
    std::shared_ptr<ObjectType> conflicting_object, original_object,
    object_at_hand;
    objectFromRevision(conflicting_revision, &conflicting_object);
    objectFromRevision(original_revision, &original_object);
    objectFromRevision(*revision_at_hand, &object_at_hand);

    if (auto_merge_policy(*conflicting_object, *original_object,
                          object_at_hand.get())) {
      objectToRevision(object_at_hand, revision_at_hand);
      return true;
    }

    return false;
  });
}

template <typename ObjectType>
void NetTable::addHeterogenousAutoMergePolicySymetrically(
    const typename AutoMergePolicy<ObjectType>::Type& auto_merge_policy) {
  addAutoMergePolicy([auto_merge_policy](const Revision& conflicting_revision,
                                         const Revision& original_revision,
                                         Revision* revision_at_hand) {
    CHECK_NOTNULL(revision_at_hand);
    std::shared_ptr<ObjectType> conflicting_object, original_object,
        object_at_hand;
    objectFromRevision(conflicting_revision, &conflicting_object);
    objectFromRevision(original_revision, &original_object);
    objectFromRevision(*revision_at_hand, &object_at_hand);

    if (auto_merge_policy(*conflicting_object, *original_object,
                          object_at_hand.get())) {
      objectToRevision(object_at_hand, revision_at_hand);
      return true;
    }

    if (auto_merge_policy(*object_at_hand, *original_object,
                          conflicting_object.get())) {
      objectToRevision(conflicting_object, revision_at_hand);
      return true;
    }

    return false;
  });
}

}  // namespace map_api

#endif  // MAP_API_NET_TABLE_INL_H_
