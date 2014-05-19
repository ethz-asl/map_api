#include "map-api/transaction.h"

#include <unordered_set>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "map-api/map-api-core.h"

DECLARE_string(ipPort);

namespace map_api {

std::recursive_mutex Transaction::dbMutex_;

bool Transaction::begin(){
  session_ = MapApiCore::getInstance().getSession();
  active_ = true;
  beginTime_ = Time();
  return true;
}

// forward declaration required, else "specialization after instantiation"
template<>
inline bool Transaction::hasContainerConflict(
    const Transaction::ConflictConditionVector& container);

bool Transaction::commit(){
  if (notifyAbortedOrInactive()){
    return false;
  }
  //return false if no jobs scheduled
  if (insertions_.empty() && updates_.empty()){
    LOG(WARNING) << "Committing transaction with no queries";
    return false;
  }
  // Acquire lock for database updates TODO(tcies) per-item locks
  {
    std::lock_guard<std::recursive_mutex> lock(dbMutex_);
    if (hasMapConflict(insertions_)) {
      VLOG(3) << "Insert conflict, commit fails";
      return false;
    }
    if (hasMapConflict(updates_)){
      VLOG(3) << "Update conflict, commit fails";
      return false;
    }
    if (hasContainerConflict(conflictConditions_)){
      VLOG(3) << "Conflict condition true, commit fails";
      return false;
    }
    // if no conflicts were found, apply changes, starting from inserts...
    // TODO(tcies) ideally, this should be rollback-able, e.g. by using
    // the SQL built-in transactions
    for (const std::pair<CRItemIdentifier, SharedRevisionPointer> &insertion :
        insertions_){
      const CRTableInterface& table = insertion.first.first;
      const Id& id = insertion.first.second;
      CRTableInterface::ItemDebugInfo debugInfo(table.name(), id);
      const SharedRevisionPointer &revision = insertion.second;
      CHECK(revision->verify("ID", id)) <<
          "Identifier ID does not match revision ID";
      if (!table.rawInsertQuery(*revision)){
        LOG(ERROR) << debugInfo << "Insertion failed, aborting commit.";
        return false;
      }
    }
    // ...then updates
    for (const std::pair<CRUItemIdentifier, SharedRevisionPointer> &update :
        updates_){
      const CRUTableInterface& table = update.first.first;
      const Id& id = update.first.second;
      CRTableInterface::ItemDebugInfo debugInfo(table.name(), id);
      const SharedRevisionPointer &revision = update.second;
      CHECK(revision->verify("ID", id)) <<
          "Identifier ID does not match revision ID";
      if (!table.rawUpdateQuery(*revision)){
        LOG(ERROR) << debugInfo << "Update failed, aborting commit.";
        return false;
      }
    }
  }
  active_ = false;
  return true;
}

bool Transaction::abort(){
  if (notifyAbortedOrInactive()){
    return false;
  }
  active_ = false;
  return true;
}

Id Transaction::insert(CRTableInterface& table,
                       const SharedRevisionPointer& item){
  Id id(Id::random());
  if (!insert(table, id, item)){
    return Id();
  }
  return id;
}


bool Transaction::insert(CRTableInterface& table, const Id& id,
                         const SharedRevisionPointer& item){
  if (notifyAbortedOrInactive()){
    return false;
  }
  if (!table.isInitialized()){
    LOG(ERROR) << "Attempted to insert into uninitialized table";
    return false;
  }
  CHECK(item) << "Passed revision pointer is null";
  std::shared_ptr<Revision> reference = table.getTemplate();
  CHECK(item->structureMatch(*reference)) <<
      "Structure of item to be inserted: " << item->DebugString() <<
      " doesn't match table template " << reference->DebugString();
  item->set("ID",id);
  // item->set("owner",owner_); TODO(tcies) later, fetch from core
  insertions_.insert(InsertMap::value_type(
      CRItemIdentifier(table, id), item));
  return true;
}

template<>
Transaction::SharedRevisionPointer Transaction::read<CRTableInterface>(
    CRTableInterface& table, const Id& id){
  if (notifyAbortedOrInactive()){
    return false;
  }
  // fast lookup in uncommitted insertions
  Transaction::CRItemIdentifier item(table, id);
  Transaction::InsertMap::iterator itemIterator = insertions_.find(item);
  if (itemIterator != insertions_.end()){
    return itemIterator->second;
  }
  std::lock_guard<std::recursive_mutex> lock(dbMutex_);
  return table.rawGetRow(id);
}

template<>
Transaction::SharedRevisionPointer Transaction::read<CRUTableInterface>(
    CRUTableInterface& table, const Id& id){
  if (notifyAbortedOrInactive()){
    return false;
  }

  // fast check in uncommitted transaction queries
  Transaction::CRUItemIdentifier cru_item(table, id);
  Transaction::UpdateMap::iterator updateIterator = updates_.find(cru_item);
  if (updateIterator != updates_.end()){
    return updateIterator->second;
  }
  Transaction::CRItemIdentifier cr_item(table, id);
  Transaction::InsertMap::iterator insertIterator = insertions_.find(cr_item);
  if (insertIterator != insertions_.end()){
    return insertIterator->second;
  }

  // TODO (tcies) per-item reader lock
  std::lock_guard<std::recursive_mutex> lock(dbMutex_);
  return table.rawGetRowAtTime(id, beginTime_);
}

template<>
bool Transaction::dumpTable<CRTableInterface>(
    CRTableInterface& table, std::vector<SharedRevisionPointer>* dest) {
  if (notifyAbortedOrInactive()){
    return false;
  }
  CHECK_NOTNULL(dest);
  dest->clear();
  {
    std::lock_guard<std::recursive_mutex> lock(dbMutex_);
    table.rawDump(dest);
  }
  // Also add yet uncommitted
  for (const std::pair<CRItemIdentifier,
      const SharedRevisionPointer> &insertion : insertions_) {
    if (insertion.first.first.name() == table.name()) {
      dest->push_back(insertion.second);
    }
  }
  // TODO(tcies) test
  // TODO(tcies) time-awareness
  return true;
}

template<>
bool Transaction::dumpTable<CRUTableInterface>(
    CRUTableInterface& table, std::vector<SharedRevisionPointer>* dest) {
  CHECK_NOTNULL(dest);
  dest->clear();
  if (notifyAbortedOrInactive()){
    return false;
  }

  // fast check in uncommitted transaction queries
  std::unordered_set<sm::HashId> updated_items;
  for (const std::pair<CRUItemIdentifier, SharedRevisionPointer> &update :
      updates_) {
    if (update.first.first.name() == table.name()){
      dest->push_back(update.second);
      updated_items.insert(update.first.second);
    }
  }
  for (const std::pair<CRItemIdentifier, SharedRevisionPointer> &insertion :
      insertions_) {
    if (insertion.first.first.name() == table.name() &&
        updated_items.find(insertion.first.second) == updated_items.end()){
      dest->push_back(insertion.second);
    }
  }

  std::lock_guard<std::recursive_mutex> lock(dbMutex_);
  // find bookkeeping rows in the table
  table.rawDumpAtTime(beginTime_, dest);
  return true;
}

bool Transaction::update(CRUTableInterface& table, const Id& id,
                         const SharedRevisionPointer& newRevision){
  if (notifyAbortedOrInactive()){
    return false;
  }
  updates_[CRUItemIdentifier(table, id)] = newRevision;
  return true;
}

// Going with locks for now TODO(tcies) adopt when moving to per-item locks
// std::shared_ptr<std::vector<proto::TableFieldDescriptor> >
// Transaction::requiredTableFields(){
//   std::shared_ptr<std::vector<proto::TableFieldDescriptor> > fields(
//       new std::vector<proto::TableFieldDescriptor>);
//   fields->push_back(proto::TableFieldDescriptor());
//   fields->back().set_name("locked_by");
//   fields->back().set_type(proto::TableFieldDescriptor_Type_HASH128);
//   return fields;
// }

bool Transaction::notifyAbortedOrInactive() const {
  if (!active_){
    LOG(ERROR) << "Transaction has not been initialized";
    return true;
  }
  if (aborted_){
    LOG(ERROR) << "Transaction has previously been aborted";
    return true;
  }
  return false;
}

/**
 * Insert requests conflict only if the id is already present
 */
template<>
bool Transaction::hasItemConflict<Transaction::CRItemIdentifier>(
    const Transaction::CRItemIdentifier& item) {
  std::lock_guard<std::recursive_mutex> lock(dbMutex_);
  // Conflict if id present in table
  if (item.first.rawGetRow(item.second)){
    LOG(WARNING) << "Table " << item.first.name() << " already contains id " <<
        item.second.hexString() << ", transaction conflict!";
    return true;
  }
  return false;
}

/**
 * Update requests conflict if there is a revision that is later than the
 * transaction begin time
 */
template<>
bool Transaction::hasItemConflict<Transaction::CRUItemIdentifier>(
    const Transaction::CRUItemIdentifier& item) {
  // no problem anyways if item inserted within same transaction
  CRItemIdentifier crItem(item.first, item.second);
  if (this->insertions_.find(crItem) != this->insertions_.end()){
    return false;
  }
  Time latestUpdate;
  if (!item.first.rawLatestUpdateTime(item.second, &latestUpdate)){
    LOG(FATAL) << "Error retrieving update time";
  }
  return latestUpdate >= beginTime_;
}


template<>
bool Transaction::hasItemConflict(
    const Transaction::ConflictCondition& item) {
  std::vector<std::shared_ptr<Revision> > results;
  return item.table.rawFindByRevision(item.key, *item.valueHolder, &results);
}

template<>
inline bool Transaction::hasContainerConflict<Transaction::InsertMap>(
    const Transaction::InsertMap& container){
  return Transaction::hasMapConflict(container);
}
template<>
inline bool Transaction::hasContainerConflict<Transaction::UpdateMap>(
    const Transaction::UpdateMap& container){
  return Transaction::hasMapConflict(container);
}
template<typename Map>
inline bool Transaction::hasMapConflict(const Map& map){
  for (const typename Map::value_type& item : map){
    if (hasItemConflict(item.first)){
      return true;
    }
  }
  return false;
}
template<>
inline bool Transaction::hasContainerConflict<
Transaction::ConflictConditionVector>(
    const Transaction::ConflictConditionVector& container){
  for (const Transaction::ConflictCondition& conflictCondition : container){
    if (hasItemConflict(conflictCondition)){
      return true;
    }
  }
  return false;
}

} /* namespace map_api */
