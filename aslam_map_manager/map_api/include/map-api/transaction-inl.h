#ifndef TRANSACTION_INL_H_
#define TRANSACTION_INL_H_

namespace map_api {

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
int Transaction::find(CRTableInterface& table, const std::string& key,
                      const ValueType& value,
                      std::vector<SharedRevisionPointer>* dest) const {
  CHECK_NOTNULL(dest);
  if (Transaction::notifyAbortedOrInactive()){
    return false;
  }
  // TODO(tcies) also browse uncommitted
  std::lock_guard<std::recursive_mutex> lock(dbMutex_);
  return table.rawFind(key, value, this->beginTime_, dest);
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
  return table.rawFindUnique(key, value, this->beginTime_);
}

template<typename ValueType>
int Transaction::findInUncommitted(
    CRTableInterface& table, const std::string& key, const ValueType& value,
    std::vector<SharedRevisionPointer>* dest) const {
  // FIXME(tcies) continue here
  return 0;
}

} // namespace map_api

#endif /* TRANSACTION_INL_H_ */
