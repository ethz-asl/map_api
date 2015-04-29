#ifndef MAP_API_LEGACY_CHUNK_DATA_CONTAINER_BASE_INL_H_
#define MAP_API_LEGACY_CHUNK_DATA_CONTAINER_BASE_INL_H_

namespace map_api {

LegacyChunkDataContainerBase::History::const_iterator
LegacyChunkDataContainerBase::History::latestAt(const LogicalTime& time) const {
  for (const_iterator it = cbegin(); it != cend(); ++it) {
    if ((*it)->getUpdateTime() <= time) {
      return it;
    }
  }
  return cend();
}

template <typename IdType>
void LegacyChunkDataContainerBase::itemHistory(const IdType& id,
                                               const LogicalTime& time,
                                               History* dest) const {
  common::Id map_api_id;
  aslam::HashId hash_id;
  id.toHashId(&hash_id);
  map_api_id.fromHashId(hash_id);
  itemHistoryImpl(map_api_id, time, dest);
}

template <typename ValueType>
void LegacyChunkDataContainerBase::findHistory(int key, const ValueType& value,
                                               const LogicalTime& time,
                                               HistoryMap* dest) const {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  if (key >= 0) {
    valueHolder->set(key, value);
  }
  return this->findHistoryByRevision(key, *valueHolder, time, dest);
}

template <typename IdType>
void LegacyChunkDataContainerBase::remove(const LogicalTime& time,
                                          const IdType& id) {
  std::shared_ptr<Revision> latest =
      std::make_shared<Revision>(*getById(id, time));
  remove(time, latest);
}

}  // namespace map_api

#endif  // MAP_API_LEGACY_CHUNK_DATA_CONTAINER_BASE_INL_H_
