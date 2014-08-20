#ifndef MAP_API_TRANSACTION_INL_H_
#define MAP_API_TRANSACTION_INL_H_

#include <string>

namespace map_api {

template <typename ValueType>
CRTable::RevisionMap Transaction::find(const std::string& key,
                                       const ValueType& value,
                                       NetTable* table) {
  CHECK_NOTNULL(table);
  return this->transactionOf(table)->find(key, value);
}

}  // namespace map_api

#endif  // MAP_API_TRANSACTION_INL_H_
