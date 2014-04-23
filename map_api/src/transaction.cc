/*
 * transaction.cc
 *
 *  Created on: Apr 3, 2014
 *      Author: titus
 */

#include <map-api/transaction.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <map-api/map-api-core.h>

DECLARE_string(ipPort);

namespace map_api {

std::mutex Transaction::dbMutex_;

Transaction::Transaction(const Hash& owner) : owner_(owner),
    active_(false), aborted_(false){
}

bool Transaction::begin(){
  session_ = MapApiCore::getInstance().getSession();
  active_ = true;
  beginTime_ = Time();
  return true;
}

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
    std::lock_guard<std::mutex> lock(dbMutex_);
    // check for conflicts in insert queues
    if (hasMapConflict(insertions_) || hasMapConflict(updates_)){
      LOG(WARNING) << "Conflict, commit fails";
      return false;
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

template<>
Hash Transaction::insert<CRTableInterface>(
    CRTableInterface& table, const SharedRevisionPointer& item){
  // TODO(tcies) item must not yet exist in InsertMap
  if (!table.IsInitialized()){
    LOG(ERROR) << "Attempted to insert into uninitialized table";
    return Hash();
  }
  if (!item){
    LOG(ERROR) << "Passed revision pointer is null";
    return Hash();
  }
  Hash idHash = Hash::randomHash();
  item->set("ID",idHash);
  item->set("owner",owner_);
  insertions_.insert(InsertMap::value_type(
      CRItemIdentifier(table, idHash), item));
  return idHash;
}

template<>
Hash Transaction::insert<CRUTableInterface>(
    CRUTableInterface& table,
    const SharedRevisionPointer& item){
  if (!table.IsInitialized()){
    LOG(ERROR) << "Attempted to insert into uninitialized table";
    return Hash();
  }
  if (!item){
    LOG(ERROR) << "Passed revision pointer is null";
    return Hash();
  }
  // 1. Prepare a CRU table entry pointing to nothing
  Hash idHash = Hash::randomHash();
  SharedRevisionPointer insertItem = table.getCRUTemplate();
  insertItem->set("ID", idHash);
  insertItem->set("owner", owner_);
  insertItem->set("latest_revision", Hash()); // invalid hash
  insertions_.insert(InsertMap::value_type(
      CRItemIdentifier(table, idHash), insertItem));

  // 2. Prepare history entry and submit to update queue
  SharedRevisionPointer updateItem =
      table.history_->prepareForInsert(*item, Hash());
  if (!updateItem){
    LOG(ERROR) << "Preparation of insert statement failed for item";
    // aborts transaction TODO(or just abort insert?)
    abort();
    return Hash();
  }
  updates_.insert(UpdateMap::value_type(
      CRUItemIdentifier(table, idHash), updateItem));
  return idHash;
}

template<>
Transaction::SharedRevisionPointer Transaction::read<CRTableInterface>(
    CRTableInterface& table, const Hash& id){
  // fast check in uncommitted insertions
  Transaction::CRItemIdentifier item(table, id);
  Transaction::InsertMap::iterator itemIterator = insertions_.find(item);
  if (itemIterator != insertions_.end()){
    return itemIterator->second;
  }
  std::lock_guard<std::mutex> lock(dbMutex_);
  return table.rawGetRow(id);
}

template<>
Transaction::SharedRevisionPointer Transaction::read<CRUTableInterface>(
    CRUTableInterface& table, const Hash& id){
  // fast check in uncommitted transaction queries
  Transaction::CRUItemIdentifier item(table, id);
  Transaction::UpdateMap::iterator itemIterator = updates_.find(item);
  if (itemIterator != updates_.end()){
    return itemIterator->second;
  }
  // TODO (tcies) per-item reader lock
  std::lock_guard<std::mutex> lock(dbMutex_);
  // find bookkeeping row
  SharedRevisionPointer cruRow = table.rawGetRow(id);
  if (!cruRow){
    LOG(ERROR) << "Can't find item " << id.getString() << " in table " <<
        table.name();
    return SharedRevisionPointer();
  }
  Hash latest;
  if (!cruRow->get("latest", &latest)){
    LOG(ERROR) << "Bookkeeping item does not contain reference to latest";
    return SharedRevisionPointer();
  }
  return table.history_->revisionAt(latest, beginTime_);
}

bool Transaction::update(CRUTableInterface& table, const Hash& id,
                         const SharedRevisionPointer& newRevision){
  updates_.insert(UpdateMap::value_type(
      CRUItemIdentifier(table, id), newRevision));
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

bool Transaction::notifyAbortedOrInactive(){
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

template<typename Map>
bool Transaction::hasMapConflict(const Map& map){
  for (const typename Map::value_type& item : map){
    if (hasItemConflict(item.first)){
      return true;
    }
  }
  return false;
}

/**
 * Insert requests conflict only if the id is already present
 */
template<>
bool Transaction::hasItemConflict<Transaction::CRItemIdentifier>(
    const Transaction::CRItemIdentifier& item){
  std::lock_guard<std::mutex> lock(dbMutex_);
  // Conflict if id present in table
  if (item.first.rawGetRow(item.second)){
    LOG(WARNING) << "Table " << item.first.name() << " already contains id " <<
        item.second.getString() << ", transaction conflict!";
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
    const Transaction::CRUItemIdentifier& item){
  // no problem anyways if item inserted within same transaction
  CRItemIdentifier crItem(item.first, item.second);
  if (this->insertions_.find(crItem) != this->insertions_.end()){
    return false;
  }
  Time latestUpdate;
  if (!item.first.rawLatestUpdate(item.second, &latestUpdate)){
    LOG(ERROR) << "Error retrieving update time";
    return true;
  }
  return latestUpdate > beginTime_;
}

} /* namespace map_api */
