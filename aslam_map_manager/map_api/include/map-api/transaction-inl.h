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

template<typename ValueType>
void Transaction::addConflictCondition(CRTableInterface& table,
                                       const std::string& key,
                                       const ValueType& value){
  SharedRevisionPointer valueHolder = table.getTemplate();
  valueHolder->set(key, value);
  Transaction::conflictconditions_.push_back(
      ConflictCondition(table, key, valueHolder));
}

} // namespace map_api

#endif /* TRANSACTION_INL_H_ */
