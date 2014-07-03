#ifndef MAP_API_LOCAL_TRANSACTION_INL_H_
#define MAP_API_LOCAL_TRANSACTION_INL_H_

namespace map_api {

template<typename ValueType>
bool LocalTransaction::addConflictCondition(const std::string& key,
                                            const ValueType& value,
                                            CRTable* table) {
  CHECK_NOTNULL(table);
  if (LocalTransaction::notifyAbortedOrInactive()) {
    return false;
  }
  SharedRevisionPointer valueHolder = table->getTemplate();
  valueHolder->set(key, value);
  LocalTransaction::conflictConditions_.push_back(
      ConflictCondition(key, valueHolder, table));
  return true;
}

template<typename ValueType>
int LocalTransaction::find(
    const std::string& key, const ValueType& value, CRTable* table,
    std::unordered_map<Id, SharedRevisionPointer>* dest) const {
  CHECK_NOTNULL(dest);
  if (LocalTransaction::notifyAbortedOrInactive()){
    return false;
  }
  dest->clear();
  findInUncommitted(*table, key, value, dest);
  if (dest->size() > 0) { // already have results in uncommited, merge from db
    std::unordered_map<Id, SharedRevisionPointer> from_database;
    {
      std::lock_guard<std::recursive_mutex> lock(dbMutex_);
      table->find(key, value, this->beginTime_, &from_database);
    }
    // this implementation of unordered_map::insert is specified to skip
    // duplicates, not override them, at least according to MSDN
    // http://msdn.microsoft.com/en-us/library/bb982322.aspx
    dest->insert(from_database.begin(), from_database.end());
  } else {  // no results in uncommitted, forward db directly
    std::lock_guard < std::recursive_mutex > lock(dbMutex_);
    table->find(key, value, this->beginTime_, dest);
  }
  return dest->size();
}

template<typename ValueType>
LocalTransaction::SharedRevisionPointer LocalTransaction::findUnique(
    const std::string& key, const ValueType& value, CRTable* table)
const {
  if (LocalTransaction::notifyAbortedOrInactive()){
    return false;
  }
  SharedRevisionPointer uncommitted =
      findUniqueInUncommitted(*table, key, value);
  if (uncommitted) {
    // uncommitted result could be an update of an item in database, so
    // verifying that the item can't be found in the database wouldn't even
    // make sense
    return uncommitted;
  }
  std::lock_guard<std::recursive_mutex> lock(dbMutex_);
  return table->findUnique(key, value, this->beginTime_);
}

template<typename ValueType>
int LocalTransaction::findInUncommitted(
    const CRTable& table, const std::string& key, const ValueType& value,
    std::unordered_map<Id, SharedRevisionPointer>* dest) const {
  CHECK_NOTNULL(dest);
  dest->clear();
  for (const std::pair<ItemId, SharedRevisionPointer> &insertion :
      insertions_) {
    if (insertion.first.table->name() == table.name()){
      if (key != "") {
        if (insertion.second->verify(key, value)) {
          (*dest)[insertion.first.id] = insertion.second;
        }
      } else {
        (*dest)[insertion.first.id] = insertion.second;
      }
    }
  }
  // possible optimization: don't browse updates if CRTable
  // (template this function or dynamic cast)
  for (const std::pair<ItemId, SharedRevisionPointer>& update : updates_) {
    if (update.first.table->name() == table.name()){
      if (key != "") {
        if (update.second->verify(key, value)) {
          (*dest)[update.first.id] = update.second;
        }
      } else {
        (*dest)[update.first.id] = update.second;
      }
    }
  }
  return dest->size();
}

template<typename ValueType>
LocalTransaction::SharedRevisionPointer
LocalTransaction::findUniqueInUncommitted(
    const CRTable& table, const std::string& key, const ValueType& value)
const {
  std::unordered_map<Id, SharedRevisionPointer> results;
  int count = this->findInUncommitted(table, key, value, &results);
  CHECK_LT(count, 2) << "Required unique find in uncommitted queries for " <<
      table.name() << ", instead got duplicates";
  if (count == 0) {
    return SharedRevisionPointer();
  } else {
    return results.begin()->second;
  }
}

} // namespace map_api

#endif /* MAP_API_LOCAL_TRANSACTION_INL_H_ */
