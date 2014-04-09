/*
 * cr-table-interface-inl.h
 *
 *  Created on: Apr 9, 2014
 *      Author: titus
 */

#ifndef CR_TABLE_INTERFACE_INL_H_
#define CR_TABLE_INTERFACE_INL_H_

namespace map_api{
template<typename Type>
bool CRTableInterface::addField(std::string name){
  return addField(name, TableField::protobufEnum<Type>());
}

}

#endif /* CR_TABLE_INTERFACE_INL_H_ */
