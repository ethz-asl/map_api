/*
 * transaction-inl.h
 *
 *  Created on: Apr 30, 2014
 *      Author: titus
 */

#ifndef TRANSACTION_INL_H_
#define TRANSACTION_INL_H_

namespace map_api {

template<typename TableInterfaceType>
Id Transaction::insert(TableInterfaceType& table,
                       const SharedRevisionPointer& item){
  Id id(Id::random());
  if (!Transaction::insert(table, id, item)){
    return Id();
  }
  return id;
}

} // namespace map_api

#endif /* TRANSACTION_INL_H_ */
