#ifndef MAP_API_CHUNK_TRANSACTION_INL_H_
#define MAP_API_CHUNK_TRANSACTION_INL_H_

#include <string>
#include <utility>
#include <vector>

#include <multiagent-mapping-common/unique-id.h>

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
  ItemTimes::const_iterator found =
      previously_committed_.find(id.template toIdType<common::Id>());
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

  // Add previously committed items.
  for (ItemTimes::const_iterator it = previously_committed_.begin();
       it != previously_committed_.end(); ++it) {
    std::shared_ptr<const Revision> item =
        chunk_->data_container_->getById(it->first, it->second);
    if (item) {  // False if item has been previously removed.
      ids->emplace(it->first.toIdType<IdType>());
    } else {
      ids->erase(it->first.toIdType<IdType>());
    }
  }
}

template <typename IdType>
bool ChunkTransaction::getMutableUpdateEntry(
    const IdType& id, std::shared_ptr<const Revision>** result) {
  CHECK_NOTNULL(result);
  // Is there already a corresponding entry in the update map?
  UpdateMap::iterator existing_entry = updates_.find(id);
  if (existing_entry != updates_.end()) {
    *result = reinterpret_cast<std::shared_ptr<const Revision>*>(
        &existing_entry->second);
    return true;
  }
  // Is there a corresponding entry in the insert map?
  InsertMap::iterator existing_insert_entry = insertions_.find(id);
  if (existing_insert_entry != insertions_.end()) {
    *result = reinterpret_cast<std::shared_ptr<const Revision>*>(
        &existing_insert_entry->second);
    return true;
  }

  // If neither, but the corresponding item exists in this chunk, create a new
  // update entry.
  // TODO(tcies) should the available id list be cached?
  std::unordered_set<IdType> ids_in_chunk;
  getAvailableIds(&ids_in_chunk);
  if (ids_in_chunk.count(id) != 0) {
    std::shared_ptr<Revision> to_emplace;
    getById(id)->copyForWrite(&to_emplace);
    std::pair<UpdateMap::iterator, bool> emplacement = updates_.insert(
        std::make_pair(id.template toIdType<common::Id>(), to_emplace));
    CHECK(emplacement.second);
    *result = reinterpret_cast<std::shared_ptr<const Revision>*>(
        &emplacement.first->second);
    return true;
  }

  return false;
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
