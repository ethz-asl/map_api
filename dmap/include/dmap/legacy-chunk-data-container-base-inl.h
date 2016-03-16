#ifndef DMAP_LEGACY_CHUNK_DATA_CONTAINER_BASE_INL_H_
#define DMAP_LEGACY_CHUNK_DATA_CONTAINER_BASE_INL_H_

namespace dmap {

LegacyChunkDataContainerBase::History::const_iterator
LegacyChunkDataContainerBase::History::latestAt(const LogicalTime& time) const {
  for (const_iterator it = cbegin(); it != cend(); ++it) {
    if ((*it)->getUpdateTime() <= time) {
      if ((*it)->isRemoved()) {
        return cend();
      } else {
        return it;
      }
    }
  }
  return cend();
}

template <typename IdType>
void LegacyChunkDataContainerBase::itemHistory(const IdType& id,
                                               const LogicalTime& time,
                                               History* dest) const {
  common::Id dmap_id;
  aslam::HashId hash_id;
  id.toHashId(&hash_id);
  dmap_id.fromHashId(hash_id);
  itemHistoryImpl(dmap_id, time, dest);
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

}  // namespace dmap

#endif  // DMAP_LEGACY_CHUNK_DATA_CONTAINER_BASE_INL_H_
