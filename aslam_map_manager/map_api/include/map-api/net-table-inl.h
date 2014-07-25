#ifndef MAP_API_NET_TABLE_INL_H_
#define MAP_API_NET_TABLE_INL_H_

namespace map_api {

template<typename ValueType>
int NetTable::findFast(
    const std::string& key, const ValueType& value, const LogicalTime& time,
    CRTable::RevisionMap* dest) {
  CHECK_NOTNULL(dest);
  int num_local_result =
      cache_->find(key, value, time, dest);
  if (num_local_result) {
    return num_local_result;
  }
  return 0;
}

} // namespace map_api

#endif /* MAP_API_NET_TABLE_INL_H_ */
