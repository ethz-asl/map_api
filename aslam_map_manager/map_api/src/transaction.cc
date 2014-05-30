#include "map-api/transaction.h"

#include <memory>
#include <unordered_set>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "map-api/map-api-core.h"

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
template<>
inline bool Transaction::hasContainerConflict(
    const Transaction::InsertMap& container);
template<>
inline bool Transaction::hasContainerConflict(
    const Transaction::UpdateMap& container);

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
    if (hasContainerConflict(insertions_)) {
      VLOG(3) << "Insert conflict, commit fails";
      return false;
    }
    if (hasContainerConflict(updates_)){
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
    for (const std::pair<ItemId, SharedRevisionPointer> &insertion :
        insertions_){
      const CRTableInterface& table = insertion.first.table;
      const Id& id = insertion.first.id;
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
    for (const std::pair<ItemId, SharedRevisionPointer> &update :
        updates_){
      const CRUTableInterface& table =
          static_cast<const CRUTableInterface&>(update.first.table);
      const Id& id = update.first.id;
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
  CHECK(insertions_.insert(std::make_pair(ItemId(table, id), item)).second)
  << "You seem to already have inserted " << ItemId(table, id);
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
  updates_.insert(std::make_pair(ItemId(table, id), newRevision));
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
  for (const std::pair<ItemId, const SharedRevisionPointer>& item :
      container){
    std::lock_guard<std::recursive_mutex> lock(dbMutex_);
    // Conflict if id present in table
    if (item.first.table.rawGetById(item.first.id, Time())){
      LOG(WARNING) << "Table " << item.first.table.name() <<
          " already contains id " << item.first.id.hexString() <<
          ", transaction conflict!";
      return true;
    }
  }
  return false;
}
template<>
inline bool Transaction::hasContainerConflict<Transaction::UpdateMap>(
    const Transaction::UpdateMap& container){
  for (const std::pair<ItemId, const SharedRevisionPointer>& item :
      container){
    const CRUTableInterface& table = static_cast<const CRUTableInterface&>(
        item.first.table);
    if (this->insertions_.find(item.first) != this->insertions_.end()){
      return false;
    }
    Time latestUpdate;
    if (!table.rawLatestUpdateTime(item.first.id, &latestUpdate)){
      LOG(FATAL) << "Error retrieving update time";
    }
    if (latestUpdate >= beginTime_) {
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
