#ifndef MAP_API_NET_TABLE_TRANSACTION_INL_H_
#define MAP_API_NET_TABLE_TRANSACTION_INL_H_

#include <string>

namespace map_api {

template <typename ValueType>
CRTable::RevisionMap NetTableTransaction::find(const std::string& key,
                                               const ValueType& value) {
  // TODO(tcies) uncommitted
  return table_->lockFind(key, value, begin_time_);
}

}  // namespace map_api

#endif  // MAP_API_NET_TABLE_TRANSACTION_INL_H_
