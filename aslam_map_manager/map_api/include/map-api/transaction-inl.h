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
bool Transaction::addConflictCondition(CRTableInterface& table,
                                       const std::string& key,
                                       const ValueType& value) {
  if (Transaction::notifyAbortedOrInactive()) {
    return false;
  }
  SharedRevisionPointer valueHolder = table.getTemplate();
  valueHolder->set(key, value);
  Transaction::conflictConditions_.push_back(
      ConflictCondition(table, key, valueHolder));
  return true;
}

template<typename ValueType>
bool Transaction::find(CRTableInterface& table, const std::string& key,
                       const ValueType& value,
                       std::vector<SharedRevisionPointer>* dest) const {
  CHECK_NOTNULL(dest);
  if (Transaction::notifyAbortedOrInactive()){
    return false;
  }
  // TODO(tcies) also browse uncommitted
  std::lock_guard<std::recursive_mutex> lock(dbMutex_);
  table.rawFind(key, value, dest);
  return true;
}

template<typename ValueType>
Transaction::SharedRevisionPointer Transaction::findUnique(
    CRTableInterface& table, const std::string& key, const ValueType& value)
const {
  if (Transaction::notifyAbortedOrInactive()){
    return false;
  }
  // TODO(tcies) also browse uncommitted
  std::lock_guard<std::recursive_mutex> lock(dbMutex_);
  return table.rawFindUnique(key, value);
}

} // namespace map_api

#endif /* TRANSACTION_INL_H_ */
