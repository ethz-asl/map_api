#ifndef MAP_API_TRANSACTION_INL_H_
#define MAP_API_TRANSACTION_INL_H_

namespace map_api {

template <typename ValueType>
void Transaction::find(const std::string& key, const ValueType& value,
          NetTable* table, CRTable::RevisionMap* result) {
  CHECK_NOTNULL(table);
  CHECK_NOTNULL(result);
  transactionOf(table)->find(key, value, result);
}

} // namespace map_api

#endif /* MAP_API_TRANSACTION_INL_H_ */
