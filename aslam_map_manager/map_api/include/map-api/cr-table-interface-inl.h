#ifndef CR_TABLE_INTERFACE_INL_H_
#define CR_TABLE_INTERFACE_INL_H_

namespace map_api{
template<typename Type>
void CRTableInterface::addField(const std::string& name){
  addField(name, Revision::protobufEnum<Type>());
}

template<typename ValueType>
int CRTableInterface::rawFind(
    const std::string& key, const ValueType& value, const Time& time,
    std::unordered_map<Id, std::shared_ptr<Revision> >* dest) const {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  if (key != "") {
    valueHolder->set(key, value);
  }
  return this->rawFindByRevision(key, *valueHolder, time, dest);
}

template<typename ValueType>
std::shared_ptr<Revision> CRTableInterface::rawFindUnique(
    const std::string& key, const ValueType& value, const Time& time) const{
  std::unordered_map<Id, std::shared_ptr<Revision>> results;
  int count = rawFind(key, value, time, &results);
  switch (count){
    case 0: return std::shared_ptr<Revision>();
    case 1: return results.begin()->second;
    default:
      LOG(FATAL) << "There seems to be more than one item with given value of "
      << key << ", table " << structure_.name();
      return std::shared_ptr<Revision>();
  }
}

} // namespace map_api

#endif /* CR_TABLE_INTERFACE_INL_H_ */
