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
int Transaction::find(
    CRTableInterface& table, const std::string& key, const ValueType& value,
    std::unordered_map<Id, SharedRevisionPointer>* dest) const {
  CHECK_NOTNULL(dest);
  if (Transaction::notifyAbortedOrInactive()){
    return false;
  }
  dest->clear();
  findInUncommitted(table, key, value, dest);
  if (dest->size() > 0) { // already have results in uncommited, merge from db
    std::unordered_map<Id, SharedRevisionPointer> from_database;
    {
      std::lock_guard<std::recursive_mutex> lock(dbMutex_);
      table.rawFind(key, value, this->beginTime_, &from_database);
    }
    // this implementation of unordered_map::insert is specified to skip
    // duplicates, not override them, at least according to MSDN
    // http://msdn.microsoft.com/en-us/library/bb982322.aspx
    dest->insert(from_database.begin(), from_database.end());
  }
  else { // no results in uncommitted, forward db directly
    std::lock_guard<std::recursive_mutex> lock(dbMutex_);
    table.rawFind(key, value, this->beginTime_, dest);
  }
  return dest->size();
}

template<typename ValueType>
Transaction::SharedRevisionPointer Transaction::findUnique(
    CRTableInterface& table, const std::string& key, const ValueType& value)
const {
  if (Transaction::notifyAbortedOrInactive()){
    return false;
  }
  SharedRevisionPointer uncommitted = findUniqueInUncommitted(
      table, key, value);
  if (uncommitted) {
    // uncommitted result could be an update of an item in database, so
    // verifying that the item can't be found in the database wouldn't even
    // make sense
    return uncommitted;
  }
  std::lock_guard<std::recursive_mutex> lock(dbMutex_);
  return table.rawFindUnique(key, value, this->beginTime_);
}

template<typename ValueType>
int Transaction::findInUncommitted(
    const CRTableInterface& table, const std::string& key,
    const ValueType& value, std::unordered_map<Id, SharedRevisionPointer>* dest)
const {
  CHECK_NOTNULL(dest);
  dest->clear();
  for (const std::pair<CRItemIdentifier, SharedRevisionPointer> &insertion :
      insertions_) {
    if (insertion.first.first.name() == table.name()){
      if (key != "") {
        if (insertion.second->verify(key, value)) {
          (*dest)[insertion.first.second] = insertion.second;
        }
      }
      else {
        (*dest)[insertion.first.second] = insertion.second;
      }
    }
  }
  // possible optimization: don't browse updates if CRTable
  // (template this function or dynamic cast)
  for (const std::pair<CRUItemIdentifier, SharedRevisionPointer> &update :
      updates_) {
    if (update.first.first.name() == table.name()){
      if (key != "") {
        if (update.second->verify(key, value)) {
          (*dest)[update.first.second] = update.second;
        }
      }
      else {
        (*dest)[update.first.second] = update.second;
      }
    }
  }
  return dest->size();
}

template<typename ValueType>
Transaction::SharedRevisionPointer Transaction::findUniqueInUncommitted(
    const CRTableInterface& table, const std::string& key,
    const ValueType& value) const {
  std::unordered_map<Id, SharedRevisionPointer> results;
  this->findInUncommitted(table, key, value, &results);
  switch(results.size()) {
    case 0: return SharedRevisionPointer();
    case 1: return results.begin()->second;
    default:
      LOG(FATAL) << "Required unique find in uncommitted queries for " <<
      table.name() << ", instead got duplicates";
      return SharedRevisionPointer();
  }
}

} // namespace map_api

#endif /* TRANSACTION_INL_H_ */
