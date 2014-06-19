#ifndef MAP_API_NET_CR_TABLE_INL_H_
#define MAP_API_NET_CR_TABLE_INL_H_

namespace map_api {

template<typename ValueType>
int NetCRTable::findFast(
    const std::string& key, const ValueType& value, const Time& time,
    std::unordered_map<Id, std::shared_ptr<Revision> >* dest) {
  CHECK_NOTNULL(dest);
  int local_result =
      cache_->find(key, value, time, dest);
  if (local_result) {
    return local_result;
  }
  std::shared_ptr<Revision> value_holder = getTemplate();
  value_holder->set(key, value);
  // TODO(tcies) implement rest
  return 0;
}

} // namespace map_api

#endif /* MAP_API_NET_CR_TABLE_INL_H_ */
