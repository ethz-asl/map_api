#ifndef CRU_TABLE_INTERFACE_INL_H_
#define CRU_TABLE_INTERFACE_INL_H_

namespace map_api{

template<typename Type>
bool CRUTableInterface::addField(const std::string& name) {
  return addField(name, Revision::protobufEnum<Type>());
}

template<typename Type>
bool CRUTableInterface::addCRUField(const std::string& name) {
  return CRTableInterface::addField(name, Revision::protobufEnum<Type>());
}

} // namespace map_api

#endif /* CRU_TABLE_INTERFACE_INL_H_ */
