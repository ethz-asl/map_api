#include "map-api/local-transaction.h"

#include <memory>
#include <unordered_set>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "map-api/map-api-core.h"

namespace map_api {

std::recursive_mutex LocalTransaction::dbMutex_;

bool LocalTransaction::begin(){
  active_ = true;
  beginTime_ = LogicalTime::sample();
  return true;
}

// forward declaration required, else "specialization after instantiation"
template<>
inline bool LocalTransaction::hasContainerConflict(
    LocalTransaction::ConflictConditionVector& container);
template<>
inline bool LocalTransaction::hasContainerConflict(
    LocalTransaction::InsertMap& container);
template<>
inline bool LocalTransaction::hasContainerConflict(
    LocalTransaction::UpdateMap& container);

bool LocalTransaction::commit(){
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
      CRTable* table = insertion.first.table;
      const Id& id = insertion.first.id;
      CRTable::ItemDebugInfo debugInfo(table->name(), id);
      const SharedRevisionPointer &revision = insertion.second;
      CHECK(revision->verify(CRTable::kIdField, id)) <<
          "Identifier ID does not match revision ID";
      if (!table->insert(revision.get())){
        LOG(ERROR) << debugInfo << "Insertion failed, aborting commit.";
        return false;
      }
    }
    // ...then updates
    for (const std::pair<ItemId, SharedRevisionPointer> &update :
        updates_){
      CRUTable* table =
          dynamic_cast<CRUTable*>(update.first.table);
      CHECK(table);
      const Id& id = update.first.id;
      CRTable::ItemDebugInfo debugInfo(table->name(), id);
      const SharedRevisionPointer &revision = update.second;
      CHECK(revision->verify(CRTable::kIdField, id)) <<
          "Identifier ID does not match revision ID";
      if (!table->update(revision.get())){
        LOG(ERROR) << debugInfo << "Update failed, aborting commit.";
        return false;
      }
    }
  }
  active_ = false;
  return true;
}

bool LocalTransaction::abort(){
  if (notifyAbortedOrInactive()){
    return false;
  }
  active_ = false;
  return true;
}

Id LocalTransaction::insert(const SharedRevisionPointer& item,
                            CRTable* table){
  CHECK_NOTNULL(table);
  Id id(Id::generate());
  if (!insert(id, item, table)){
    return Id();
  }
  return id;
}


bool LocalTransaction::insert(
    const Id& id, const SharedRevisionPointer& item, CRTable* table){
  CHECK_NOTNULL(table);
  if (notifyAbortedOrInactive()){
    return false;
  }
  if (!table->isInitialized()){
    LOG(ERROR) << "Attempted to insert into uninitialized table";
    return false;
  }
  CHECK(item) << "Passed revision pointer is null";
  std::shared_ptr<Revision> reference = table->getTemplate();
  CHECK(item->structureMatch(*reference)) <<
      "Structure of item to be inserted: " << item->DebugString() <<
      " doesn't match table template " << reference->DebugString();
  item->set(CRTable::kIdField, id);
  // item->set("owner",owner_); TODO(tcies) later, fetch from core
  CHECK(insertions_.insert(std::make_pair(ItemId(id, table), item)).second)
  << "You seem to already have inserted " << ItemId(id, table);
  return true;
}

LocalTransaction::SharedRevisionPointer LocalTransaction::read(
    const Id& id, CRTable* table){
  CHECK_NOTNULL(table);
  return findUnique(CRTable::kIdField, id, table);
}

bool LocalTransaction::dumpTable(
    CRTable* table, std::unordered_map<Id, SharedRevisionPointer>* dest) {
  CHECK_NOTNULL(table);
  CHECK_NOTNULL(dest);
  return find("", 0, table, dest);
}

bool LocalTransaction::update(
    const Id& id, const SharedRevisionPointer& newRevision, CRUTable* table){
  CHECK_NOTNULL(table);
  if (notifyAbortedOrInactive()){
    return false;
  }
  updates_.insert(std::make_pair(ItemId(id, table), newRevision));
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

bool LocalTransaction::notifyAbortedOrInactive() const {
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
bool LocalTransaction::hasItemConflict(
    LocalTransaction::ConflictCondition& item) {
  std::unordered_map<Id, SharedRevisionPointer> results;
  return item.table->findByRevision(item.key, *item.valueHolder,
                                    LogicalTime::sample(), &results);
}

template<>
inline bool LocalTransaction::hasContainerConflict<LocalTransaction::InsertMap>(
    LocalTransaction::InsertMap& container){
  for (const std::pair<ItemId, const SharedRevisionPointer>& item :
      container){
    std::lock_guard<std::recursive_mutex> lock(dbMutex_);
    // Conflict if id present in table
    if (item.first.table->getById(item.first.id, LogicalTime::sample())){
      LOG(WARNING) << "Table " << item.first.table->name() <<
          " already contains id " << item.first.id.hexString() <<
          ", transaction conflict!";
      return true;
    }
  }
  return false;
}
template<>
inline bool LocalTransaction::hasContainerConflict<LocalTransaction::UpdateMap>(
    LocalTransaction::UpdateMap& container){
  for (const std::pair<ItemId, const SharedRevisionPointer>& item :
      container){
    try {
      CRUTable* table = dynamic_cast<CRUTable*>(item.first.table);
      CHECK(table);
      if (this->insertions_.find(item.first) != this->insertions_.end()){
        return false;
      }
      LogicalTime latest_update;
      if (!table->getLatestUpdateTime(item.first.id, &latest_update)){
        LOG(FATAL) << "Error retrieving update time";
      }
      if (latest_update >= beginTime_) {
        return true;
      }
    } catch (const std::bad_cast& e) {
      LOG(FATAL) << "Cast to CRUTableInterface reference failed";
    }
  }
  return false;
}
template<>
inline bool LocalTransaction::hasContainerConflict<
LocalTransaction::ConflictConditionVector>(
    LocalTransaction::ConflictConditionVector& container){
  for (LocalTransaction::ConflictCondition& conflictCondition :
      container){
    if (hasItemConflict(conflictCondition)){
      return true;
    }
  }
  return false;
}

} /* namespace map_api */
