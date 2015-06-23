#ifndef MAP_API_CHUNK_TRANSACTION_INL_H_
#define MAP_API_CHUNK_TRANSACTION_INL_H_

#include <string>
#include <vector>

namespace map_api {

template <typename ValueType>
void ChunkTransaction::addConflictCondition(int key, const ValueType& value) {
  std::shared_ptr<Revision> value_holder =
      chunk_->data_container_->getTemplate();
  value_holder->set(key, value);
  conflict_conditions_.push_back(ConflictCondition(key, value_holder));
}

template <typename IdType>
std::shared_ptr<const Revision> ChunkTransaction::getById(const IdType& id) {
  std::shared_ptr<const Revision> result = getByIdFromUncommitted(id);
  if (result != nullptr) {
    return result;
  }

  LogicalTime get_time = begin_time_;
  common::Id common_id;
  aslam::HashId hash_id;
  id.toHashId(&hash_id);
  common_id.fromHashId(hash_id);
  ItemTimes::const_iterator found = previously_committed_.find(common_id);
  if (found != previously_committed_.end()) {
    get_time = found->second;
  }

  chunk_->readLock();
  result = chunk_->data_container_->getById(id, get_time);
  chunk_->unlock();
  return result;
}

template <typename IdType>
void ChunkTransaction::getAvailableIds(std::unordered_set<IdType>* ids) {
  CHECK_NOTNULL(ids)->clear();
  std::vector<IdType> id_vector;
  chunk_->constData()->getAvailableIds(begin_time_, &id_vector);
  ids->insert(id_vector.begin(), id_vector.end());
  // Remove items removed in previous commits.
  for (ItemTimes::const_iterator it = previously_committed_.begin();
       it != previously_committed_.end(); ++it) {
    IdType id;
    aslam::HashId hash_id;
    it->first.toHashId(&hash_id);
    id.fromHashId(hash_id);
    typename std::unordered_set<IdType>::iterator found = ids->find(id);
    if (found != ids->end()) {
      std::shared_ptr<const Revision> item =
          chunk_->data_container_->getById(it->first, it->second);
      if (!item) {
        ids->erase(found);
      }
    }
  }
}

template <typename IdType>
std::shared_ptr<const Revision> ChunkTransaction::getByIdFromUncommitted(
    const IdType& id) const {
  UpdateMap::const_iterator updated = updates_.find(id);
  if (updated != updates_.end()) {
    return updated->second;
  }
  InsertMap::const_iterator inserted = insertions_.find(id);
  if (inserted != insertions_.end()) {
    return inserted->second;
  }
  return std::shared_ptr<Revision>();
}

template <typename ValueType>
std::shared_ptr<const Revision> ChunkTransaction::findUnique(
    int key, const ValueType& value) {
  // FIXME(tcies) Also search in uncommitted.
  // FIXME(tcies) Also search in previously committed.
  chunk_->readLock();
  std::shared_ptr<const Revision> result =
      chunk_->data_container_->findUnique(key, value, begin_time_);
  chunk_->unlock();
  return result;
}

}  // namespace map_api

#endif  // MAP_API_CHUNK_TRANSACTION_INL_H_
