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
      getByIdInconsistent(id, LogicalTime::sample());
  registerChunkInSpace(item_revision->getChunkId(), bounding_box);
}

template <typename ValueType>
CRTable::RevisionMap NetTable::lockFind(int key, const ValueType& value,
                                        const LogicalTime& time) {
  CRTable::RevisionMap result;
  readLockActiveChunks();
  cache_->find(key, value, time, &result);
  unlockActiveChunks();
  return result;
}

template <typename IdType>
std::shared_ptr<const Revision> NetTable::getByIdInconsistent(
    const IdType& id, const LogicalTime& time) {
  return cache_->getById(id, time);
}

template <typename IdType>
void NetTable::getAvailableIds(const LogicalTime& time,
                               std::vector<IdType>* ids) {
  CHECK_NOTNULL(ids);
  ids->clear();
  readLockActiveChunks();
  cache_->getAvailableIds(time, ids);
  unlockActiveChunks();
}

template <typename TrackeeType, typename TrackerType>
const std::function<Id(const Revision&)>& NetTable::trackerDeterminerFactory() {
  return [](const Revision& trackee_revision) {  // NOLINT
    std::shared_ptr<TrackeeType> trackee =
        objectFromRevision<TrackeeType>(trackee_revision);
    return determineTracker<TrackeeType, TrackerType>(*trackee);
  };
}

template <typename TrackeeType, typename TrackerType>
void NetTable::pushNewChunkIdsToTrackingItem() {
  NetTable* tracker_table = tableForType<TrackerType>();
  this->pushNewChunkIdsToTrackingItem(
      tracker_table, trackerDeterminerFactory<TrackeeType, TrackerType>());
}

}  // namespace map_api

#endif  // MAP_API_NET_TABLE_INL_H_
