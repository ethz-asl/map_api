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
std::function<common::Id(const Revision&)>
NetTable::trackerDeterminerFactory() {
  return [](const Revision& trackee_revision) {  // NOLINT
    std::shared_ptr<TrackeeType> trackee =
        objectFromRevision<TrackeeType>(trackee_revision);
    TrackerIdType typed_tracker_id =
        determineTracker<TrackeeType, TrackerType, TrackerIdType>(*trackee);
    return static_cast<common::Id>(typed_tracker_id);
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
void NetTable::followTrackedChunksOfItem(const common::Id& item_id,
                                         ChunkBase* tracker_chunk);

template <typename IdType>
void NetTable::followTrackedChunksOfItem(const IdType& item_id,
                                         ChunkBase* tracker_chunk) {
  common::Id common_id;
  common_id.fromHashId(item_id.toHashId());
  followTrackedChunksOfItem(item_id, tracker_chunk);
}

}  // namespace map_api

#endif  // MAP_API_NET_TABLE_INL_H_
