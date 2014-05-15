#ifndef CR_TABLE_INTERFACE_INL_H_
#define CR_TABLE_INTERFACE_INL_H_

namespace map_api{
template<typename Type>
void CRTableInterface::addField(const std::string& name){
  addField(name, Revision::protobufEnum<Type>());
}

template<typename ValueType>
int CRTableInterface::rawFind(const std::string& key, const ValueType& value,
            std::vector<std::shared_ptr<Revision> >* dest) const {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  valueHolder->set(key, value);
  return this->rawFindByRevision(key, *valueHolder, dest);
}

template<typename ValueType>
std::shared_ptr<Revision> CRTableInterface::rawFindUnique(
    const std::string& key, const ValueType& value) const{
  std::vector<std::shared_ptr<Revision> > results;
  int count = rawFind(key, value, &results);
  switch (count){
    case 0: return std::shared_ptr<Revision>();
    case 1: return results[0];
    default:
      LOG(FATAL) << "There seems to be more than one item with given value of "
      << key << ", table " << name();
      return std::shared_ptr<Revision>();
  }
}

} // namespace map_api

#endif /* CR_TABLE_INTERFACE_INL_H_ */
