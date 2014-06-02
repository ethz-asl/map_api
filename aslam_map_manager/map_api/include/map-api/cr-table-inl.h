#ifndef MAP_API_CR_TABLE_INL_H_
#define MAP_API_CR_TABLE_INL_H_

namespace map_api{
template<typename Type>
void CRTable::addField(const std::string& name){
  addField(name, Revision::protobufEnum<Type>());
}

template<typename ValueType>
int CRTable::rawFind(const std::string& key, const ValueType& value,
                     const Time& time, std::unordered_map<Id,
                     std::shared_ptr<Revision> >* dest) const {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  if (key != "") {
    valueHolder->set(key, value);
  }
  return this->rawFindByRevision(key, *valueHolder, time, dest);
}

template<typename ValueType>
std::shared_ptr<Revision> CRTable::rawFindUnique(
    const std::string& key, const ValueType& value, const Time& time) const{
  std::unordered_map<Id, std::shared_ptr<Revision>> results;
  int count = rawFind(key, value, time, &results);
  CHECK_LT(count, 2) << "There seems to be more than one item with given"\
      " value of " << key << ", table " << structure_.name();
  if (count == 0) {
    return std::shared_ptr<Revision>();
  } else {
    return results.begin()->second;
  }
}

template<typename ClassType>
ClassType& CRTable::meyersInstance() {
  static ClassType object;
  return object;
}

} // namespace map_api

#endif /* MAP_API_CR_TABLE_INL_H_ */
