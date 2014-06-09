#ifndef MAP_API_CR_TABLE_INL_H_
#define MAP_API_CR_TABLE_INL_H_

namespace map_api{

template<typename ValueType>
int CRTable::find(const std::string& key, const ValueType& value,
                     const Time& time, std::unordered_map<Id,
                     std::shared_ptr<Revision> >* dest) {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  if (key != "") {
    valueHolder->set(key, value);
  }
  return this->findByRevision(key, *valueHolder, time, dest);
}

template<typename ValueType>
std::shared_ptr<Revision> CRTable::findUnique(
    const std::string& key, const ValueType& value, const Time& time) {
  std::unordered_map<Id, std::shared_ptr<Revision>> results;
  int count = find(key, value, time, &results);
  CHECK_LT(count, 2) << "There seems to be more than one item with given"\
      " value of " << key << ", table " << descriptor_->name();
  if (count == 0) {
    return std::shared_ptr<Revision>();
  } else {
    return results.begin()->second;
  }
}

} // namespace map_api

#endif /* MAP_API_CR_TABLE_INL_H_ */
