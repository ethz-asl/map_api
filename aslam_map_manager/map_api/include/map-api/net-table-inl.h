#ifndef MAP_API_NET_TABLE_INL_H_
#define MAP_API_NET_TABLE_INL_H_

#include <string>

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
CRTable::RevisionMap NetTable::lockFind(const std::string& key,
                                        const ValueType& value,
                                        const LogicalTime& time) {
  CRTable::RevisionMap result;
  readLockActiveChunks();
  cache_->find(key, value, time, &result);
  unlockActiveChunks();
  return result;
}

template <typename IdType>
std::shared_ptr<const Revision> NetTable::getByIdInconsistent(
    const UniqueId<IdType>& id, const LogicalTime& time) {
  return cache_->getById(id, time);
}

template <typename IdType>
void NetTable::getAvailableIds(const LogicalTime& time,
                               std::unordered_set<IdType>* ids) {
  CHECK_NOTNULL(ids);
  ids->clear();
  readLockActiveChunks();
  cache_->getAvailableIds(time, ids);
  unlockActiveChunks();
}

}  // namespace map_api

#endif  // MAP_API_NET_TABLE_INL_H_
