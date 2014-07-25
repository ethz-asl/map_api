#ifndef MAP_API_NET_TABLE_TRANSACTION_INL_H_
#define MAP_API_NET_TABLE_TRANSACTION_INL_H_

namespace map_api {

template <typename ValueType>
void NetTableTransaction::find(const std::string& key, const ValueType& value,
                               CRTable::RevisionMap* result) {
  CHECK_NOTNULL(result);
  // FIXME(tcies) find in uncommitted!
  table_->findFast(key, value, begin_time_, result);
}

} // namespace map_api

#endif /* MAP_API_NET_TABLE_TRANSACTION_INL_H_ */
