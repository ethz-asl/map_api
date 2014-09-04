#ifndef MAP_API_NET_TABLE_TRANSACTION_INL_H_
#define MAP_API_NET_TABLE_TRANSACTION_INL_H_

#include <string>

namespace map_api {

template <typename IdType>
std::shared_ptr<Revision> NetTableTransaction::getById(const IdType& id) {
  Chunk* chunk = chunkOf(id);
  return getById(id, chunk);
}

template <typename IdType>
std::shared_ptr<Revision> NetTableTransaction::getById(const IdType& id,
                                                       Chunk* chunk) {
  CHECK_NOTNULL(chunk);
  return transactionOf(chunk)->getById(id);
}

template <typename ValueType>
CRTable::RevisionMap NetTableTransaction::find(const std::string& key,
                                               const ValueType& value) {
  // TODO(tcies) uncommitted
  return table_->lockFind(key, value, begin_time_);
}

template <typename IdType>
void NetTableTransaction::getAvailableIds(std::unordered_set<IdType>* ids) {
  CHECK_NOTNULL(ids);
  table_->getAvailableIds(begin_time_, ids);
}

template <typename IdType>
Chunk* NetTableTransaction::chunkOf(const IdType& id) {
  // TODO(tcies) uncommitted
  // using the latest logical time ensures fastest lookup
  std::shared_ptr<Revision> latest = table_->getByIdInconsistent(id);
  Id chunk_id;
  latest->get(NetTable::kChunkIdField, &chunk_id);
  return table_->getChunk(chunk_id);
}

}  // namespace map_api

#endif  // MAP_API_NET_TABLE_TRANSACTION_INL_H_
