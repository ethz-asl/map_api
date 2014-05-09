#ifndef CR_TABLE_INTERFACE_INL_H_
#define CR_TABLE_INTERFACE_INL_H_

namespace map_api{
template<typename Type>
bool CRTableInterface::addField(const std::string& name){
  return addField(name, Revision::protobufEnum<Type>());
}

} // namespace map_api

#endif /* CR_TABLE_INTERFACE_INL_H_ */
