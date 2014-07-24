#ifndef MAP_API_CHUNK_TRANSACTION_INL_H_
#define MAP_API_CHUNK_TRANSACTION_INL_H_

namespace map_api {

template <typename ValueType>
void ChunkTransaction::addConflictCondition(
    const std::string& key, const ValueType& value) {
  std::shared_ptr<Revision> value_holder = cache_->getTemplate();
  value_holder->set(key, value);
  conflict_conditions_.push_back(ConflictCondition(key, value_holder));
}

} // namespace map_api

#endif /* MAP_API_CHUNK_TRANSACTION_INL_H_ */
