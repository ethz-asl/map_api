#ifndef MAP_API_NET_TABLE_INL_H_
#define MAP_API_NET_TABLE_INL_H_

#include <string>

namespace map_api {

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
std::shared_ptr<Revision> NetTable::getByIdInconsistent(const IdType& id) {
  return cache_->getById(id, LogicalTime::sample());
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
