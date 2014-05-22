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
      CHECK(revision->verify(CRTableInterface::kIdField, id)) <<
          "Identifier ID does not match revision ID";
      if (!table.rawInsert(*revision)){
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
      CHECK(revision->verify(CRTableInterface::kIdField, id)) <<
          "Identifier ID does not match revision ID";
      if (!table.rawUpdate(*revision)){
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
  item->set(CRTableInterface::kIdField, id);
  // item->set("owner",owner_); TODO(tcies) later, fetch from core
  insertions_.insert(InsertMap::value_type(
      CRItemIdentifier(table, id), item));
  return true;
}

Transaction::SharedRevisionPointer Transaction::read(
    CRTableInterface& table, const Id& id){
  return findUnique(table, CRTableInterface::kIdField, id);
}

bool Transaction::dumpTable(CRTableInterface& table,
                            std::unordered_map<Id, SharedRevisionPointer>* dest) {
  return find(table, "", 0, dest);
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
  if (item.first.rawGetById(item.second, Time())){
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
  std::unordered_map<Id, SharedRevisionPointer> results;
  return item.table.rawFindByRevision(item.key, *item.valueHolder, Time(),
                                      &results);
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
