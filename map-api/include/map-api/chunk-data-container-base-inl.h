#ifndef MAP_API_CHUNK_DATA_CONTAINER_BASE_INL_H_
#define MAP_API_CHUNK_DATA_CONTAINER_BASE_INL_H_

#include <sstream>  // NOLINT
#include <utility>
#include <vector>

#include "map-api/revision-map.h"

namespace map_api {

template <typename IdType>
std::shared_ptr<const Revision> ChunkDataContainerBase::getById(
    const IdType& id, const LogicalTime& time) const {
  std::lock_guard<std::mutex> lock(access_mutex_);
  CHECK(isInitialized()) << "Attempted to getById from non-initialized table";
  CHECK(id.isValid()) << "Supplied invalid ID";
  common::Id map_api_id;
  aslam::HashId hash_id;
  id.toHashId(&hash_id);
  map_api_id.fromHashId(hash_id);
  return getByIdImpl(map_api_id, time);
}

template <typename ValueType>
void ChunkDataContainerBase::find(int key, const ValueType& value,
                                  const LogicalTime& time,
                                  ConstRevisionMap* dest) const {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  if (key >= 0) {
    valueHolder->set(key, value);
  }
  this->findByRevision(key, *valueHolder, time, dest);
}

template <typename ValueType>
std::shared_ptr<const Revision> ChunkDataContainerBase::findUnique(
    int key, const ValueType& value, const LogicalTime& time) const {
  ConstRevisionMap results;
  find(key, value, time, &results);
  int count = results.size();
  if (count > 1) {
    std::stringstream report;
    report << "There seems to be more than one (" << count
           << ") item with given"
              " value of " << key << ", table " << descriptor_->name()
           << std::endl;
    report << "Items found at " << time << " are:" << std::endl;
    for (const ConstRevisionMap::value_type result : results) {
      report << result.second->dumpToString() << std::endl;
    }
    LOG(FATAL) << report.str();
    return std::shared_ptr<const Revision>();
  } else if (count == 0) {
    return std::shared_ptr<Revision>();
  } else {
    return results.begin()->second;
  }
}

template <typename IdType>
void ChunkDataContainerBase::getAvailableIds(const LogicalTime& time,
                                             std::vector<IdType>* ids) const {
  std::lock_guard<std::mutex> lock(access_mutex_);
  CHECK(isInitialized()) << "Attempted to getById from non-initialized table";
  CHECK_NOTNULL(ids);
  ids->clear();
  std::vector<common::Id> map_api_ids;
  getAvailableIdsImpl(time, &map_api_ids);
  ids->reserve(map_api_ids.size());
  for (const common::Id& id : map_api_ids) {
    ids->emplace_back(id.toIdType<IdType>());
  }
}

template <typename ValueType>
int ChunkDataContainerBase::count(int key, const ValueType& value,
                                  const LogicalTime& time) const {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  CHECK(valueHolder != nullptr);
  if (key >= 0) {
    valueHolder->set(key, value);
  }
  return this->countByRevision(key, *valueHolder, time);
}

}  // namespace map_api

#endif  // MAP_API_CHUNK_DATA_CONTAINER_BASE_INL_H_
