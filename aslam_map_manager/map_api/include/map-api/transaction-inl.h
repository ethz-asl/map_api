#ifndef MAP_API_TRANSACTION_INL_H_
#define MAP_API_TRANSACTION_INL_H_
#include <string>
#include <vector>

namespace map_api {

template <typename ValueType>
CRTable::RevisionMap Transaction::find(int key, const ValueType& value,
                                       NetTable* table) {
  CHECK_NOTNULL(table);
  return this->transactionOf(table)->find(key, value);
}

template <typename IdType>
std::shared_ptr<const Revision> Transaction::getById(const IdType& id,
                                                     NetTable* table) const {
  CHECK_NOTNULL(table);
  return transactionOf(table)->getById(id);
}

template <typename IdType>
std::shared_ptr<const Revision> Transaction::getById(const IdType& id,
                                                     NetTable* table,
                                                     Chunk* chunk) const {
  CHECK_NOTNULL(table);
  CHECK_NOTNULL(chunk);
  return transactionOf(table)->getById(id, chunk);
}

template <typename IdType>
void Transaction::getAvailableIds(NetTable* table,
                                  std::vector<IdType>* ids) {
  return transactionOf(CHECK_NOTNULL(table))
      ->getAvailableIds(CHECK_NOTNULL(ids));
}

template <typename IdType>
void Transaction::remove(NetTable* table, const UniqueId<IdType>& id) {
  return transactionOf(CHECK_NOTNULL(table))->remove(id);
}

}  // namespace map_api

#endif  // MAP_API_TRANSACTION_INL_H_
