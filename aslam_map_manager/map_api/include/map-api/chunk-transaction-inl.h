#ifndef MAP_API_CHUNK_TRANSACTION_INL_H_
#define MAP_API_CHUNK_TRANSACTION_INL_H_

#include <string>

namespace map_api {

template <typename ValueType>
void ChunkTransaction::addConflictCondition(
    const std::string& key, const ValueType& value) {
  std::shared_ptr<Revision> value_holder =
      chunk_->underlying_table_->getTemplate();
  value_holder->set(key, value);
  conflict_conditions_.push_back(ConflictCondition(key, value_holder));
}

template <typename ValueType>
std::shared_ptr<Revision> ChunkTransaction::findUnique(
    const std::string& key, const ValueType& value) {
  // FIXME(tcies) also search in uncommitted
  std::shared_ptr<Revision> result;
  chunk_->readLock();
  result = chunk_->underlying_table_->findUnique(key, value, begin_time_);
  chunk_->unlock();
  return result;
}

}  // namespace map_api

#endif  // MAP_API_CHUNK_TRANSACTION_INL_H_
