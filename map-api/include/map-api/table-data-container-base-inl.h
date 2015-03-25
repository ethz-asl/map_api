#ifndef MAP_API_TABLE_DATA_CONTAINER_BASE_INL_H_
#define MAP_API_TABLE_DATA_CONTAINER_BASE_INL_H_

#include <sstream>  // NOLINT
#include <utility>
#include <vector>

#include "map-api/revision-map.h"

namespace map_api {

template <typename IdType>
std::shared_ptr<const Revision> TableDataContainerBase::getById(
    const IdType& id, const LogicalTime& time) const {
  std::lock_guard<std::mutex> lock(access_mutex_);
  CHECK(isInitialized()) << "Attempted to getById from non-initialized table";
  CHECK(id.isValid()) << "Supplied invalid ID";
  common::Id map_api_id;
  sm::HashId hash_id;
  id.toHashId(&hash_id);
  map_api_id.fromHashId(hash_id);
  return getByIdImpl(map_api_id, time);
}

template <typename ValueType>
void TableDataContainerBase::find(int key, const ValueType& value,
                                  const LogicalTime& time,
                                  ConstRevisionMap* dest) const {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  if (key >= 0) {
    valueHolder->set(key, value);
  }
  this->findByRevision(key, *valueHolder, time, dest);
}

template <typename ValueType>
std::shared_ptr<const Revision> TableDataContainerBase::findUnique(
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

TableDataContainerBase::History::const_iterator
TableDataContainerBase::History::latestAt(const LogicalTime& time) const {
  for (const_iterator it = cbegin(); it != cend(); ++it) {
    if ((*it)->getUpdateTime() <= time) {
      return it;
    }
  }
  return cend();
}

template <typename IdType>
void TableDataContainerBase::itemHistory(const IdType& id,
                                         const LogicalTime& time,
                                         History* dest) const {
  common::Id map_api_id;
  sm::HashId hash_id;
  id.toHashId(&hash_id);
  map_api_id.fromHashId(hash_id);
  itemHistoryImpl(map_api_id, time, dest);
}

template <typename ValueType>
void TableDataContainerBase::findHistory(int key, const ValueType& value,
                                         const LogicalTime& time,
                                         HistoryMap* dest) const {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  if (key >= 0) {
    valueHolder->set(key, value);
  }
  return this->findHistoryByRevision(key, *valueHolder, time, dest);
}

template <typename IdType>
void TableDataContainerBase::remove(const LogicalTime& time, const IdType& id) {
  std::shared_ptr<Revision> latest =
      std::make_shared<Revision>(*getById(id, time));
  remove(time, latest);
}

template <typename IdType>
void TableDataContainerBase::getAvailableIds(const LogicalTime& time,
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
int TableDataContainerBase::count(int key, const ValueType& value,
                                  const LogicalTime& time) const {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  CHECK(valueHolder != nullptr);
  if (key >= 0) {
    valueHolder->set(key, value);
  }
  return this->countByRevision(key, *valueHolder, time);
}

}  // namespace map_api

#endif  // MAP_API_TABLE_DATA_CONTAINER_BASE_INL_H_
