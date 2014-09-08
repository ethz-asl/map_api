#ifndef MAP_API_NET_TABLE_TRANSACTION_INL_H_
#define MAP_API_NET_TABLE_TRANSACTION_INL_H_

#include <string>

namespace map_api {

template <typename IdType>
std::shared_ptr<Revision> NetTableTransaction::getById(const IdType& id) {
  std::shared_ptr<Revision> result;
  Chunk* chunk = chunkOf(id, &result);
  if (chunk) {
    LogicalTime inconsistent_latest_time, chunk_latest_commit;
    if (table_->type() == CRTable::Type::CR) {
      result->get(CRTable::kInsertTimeField, &inconsistent_latest_time);
    } else {
      CHECK(table_->type() == CRTable::Type::CRU);
      result->get(CRUTable::kUpdateTimeField, &inconsistent_latest_time);
    }
    chunk_latest_commit = chunk->getLatestCommitTime();
    if (chunk_latest_commit <= inconsistent_latest_time) {
      return result;
    } else {
      // TODO(tcies) another optimization possibility: item dug deep in
      // history anyways, so not affected be new updates
      return getById(id, chunk);
    }
  } else {
    LOG(ERROR) << "Item " << id << " from table " << table_->name()
               << " not present in active chunks";
    return std::shared_ptr<Revision>();
  }
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
Chunk* NetTableTransaction::chunkOf(const IdType& id,
                                    std::shared_ptr<Revision>* latest) {
  CHECK_NOTNULL(latest);
  // TODO(tcies) uncommitted
  // using the latest logical time ensures fastest lookup
  *latest = table_->getByIdInconsistent(id);
  if (!(*latest)) {
    return nullptr;
  }
  Id chunk_id;
  (*latest)->get(NetTable::kChunkIdField, &chunk_id);
  return table_->getChunk(chunk_id);
}

}  // namespace map_api

#endif  // MAP_API_NET_TABLE_TRANSACTION_INL_H_
