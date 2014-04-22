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
    CRTableInterface& table,
    const SharedRevisionPointer& item){
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
  if (itemIterator != cruUpdateState_.end()){
    return itemIterator->second->second;
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
  Hash latest;
  // if in uncommitted queue, check for latest there
  CRUItemIdentifier item(table, id);
  CRUUpdateState::iterator itemIterator = cruUpdateState_.find(item);
  if (itemIterator != cruUpdateState_.end()){
    if (!itemIterator->second->second->get("ID", &latest)){
      LOG(FATAL) << "cruUpdateState_ entry has no field ID!";
    }
  }
  else {
    // get latest from history as previous
    Hash absoluteLatest;
    SharedRevisionPointer cruRevision;
    {
      std::lock_guard<std::mutex> lock(dbMutex_);
      cruRevision = table.rawGetRow(id);
    }
    if (!cruRevision){
      LOG(ERROR) << "Couldn't find item " << id.getString() <<
          " referred to by update";
      return false;
    }
    if (!cruRevision->get("latest_revision", &absoluteLatest)){
      LOG(FATAL) << "CRU table seems to miss 'latest_revision'";
    }
    SharedRevisionPointer historyLatest = table.history_->revisionAt(
        absoluteLatest, beginTime_);
    if (!historyLatest){
      LOG(ERROR) << "Failed to retrieve latest revision before transaction "\
          "begin time from history for item " << id.getString() << " from table"
          << table.name();
      return false;
    }
    if (!historyLatest->get("ID", &latest)){
      LOG(ERROR) << "Latest history entry for seems to have no 'ID'";
      return false;
    }
  }
  // TODO(tcies) NO! Just push revision as updaterequest, determine "previous"
  // only at commit time!!!
  // insert request to history
  insert<CRTableInterface>(*table.history_, table.history_->prepareForInsert(
      *newRevision, latest));
  // update request to CRU table
  return false;
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

template<typename Queue, typename UpdateState>
bool Transaction::hasQueueConflict(const Queue& queue, UpdateState& state){
  for (const typename Queue::value_type& request : queue){
    if (hasRequestConflict(request, state)){
      return true;
    }
  }
  return false;
}

/**
 * CR insert requests conflict only if the id is already present
 */
template<>
bool Transaction::hasRequestConflict<Transaction::CRInsertRequest,
Transaction::CRUpdateState>(Transaction::CRInsertRequest& request,
                            Transaction::CRUpdateState& state){
  Hash id;
  if (hasInsertRequestConflictCommons<Transaction::CRInsertRequest,
      Transaction::CRUpdateState, Transaction::CRItemIdentifier>(
          request, state, id)){
    return true;
  }
  // Register insert for stateful conflict checking
  Transaction::CRItemIdentifier item(request.first, id);
  state[item] = &request;
  return false;
}
/**
 * CRU insert request: very similar to CR insert request
 */
template<>
bool Transaction::hasRequestConflict<Transaction::CRUInsertRequest,
Transaction::CRUUpdateState>(Transaction::CRUInsertRequest& request,
                             Transaction::CRUUpdateState& state){
  Hash id;
  if (hasInsertRequestConflictCommons<Transaction::CRUInsertRequest,
      Transaction::CRUUpdateState, Transaction::CRUItemIdentifier>(
          request, state, id)){
    return true;
  }
  // Register insert for stateful conflict checking
  Transaction::CRUItemIdentifier item(request.first, id);
  state[item] = NULL;
  return false;
}

/**
 * Update requests conflict if there is a revision that is later than the one
 * referenced as "previous" in the update request.
 */
template<>
bool Transaction::hasRequestConflict<Transaction::UpdateRequest,
Transaction::CRUUpdateState>(Transaction::UpdateRequest& request,
                             Transaction::CRUUpdateState& state){
  const CRUItemIdentifier& item = request.first;
  const SharedRevisionPointer& revision = request.second;
  // "previous" element intended by the update request
  Hash intendedPrevious, previous;
  if (!revision->get("previous", &intendedPrevious)){
    LOG(ERROR) << "Queued history item does not contain reference to previous "\
        "revision";
    return true;
  }
  Transaction::CRUUpdateState::iterator itemIterator = state.find(item);
  if (itemIterator == state.end()){
    // item has not been previously updated in this transaction
    const CRUTableInterface& table = item.first;
    const Hash& id = item.second;
    SharedRevisionPointer currentRow = table.rawGetRow(id);
    if (!currentRow){
      LOG(WARNING) << "Element to be updated seems not to exist";
      return true;
    }
    if (!currentRow->get("latest_revision", &previous)){
      LOG(ERROR) << "CRU table " << table.name() << " seems to miss "\
          "'latest_revision' column";
      return true;
    }
  }
  else{
    // item has been previously updated in this transaction
    if (itemIterator->second->second){
      if (!(itemIterator->second->second->get("ID", &previous))){
        LOG(ERROR) << "Revision in conflict check update state seems to miss "\
            "'ID'";
        return true;
      }
    }
    // if itemIterator->second is NULL, previous remains empty Hash
  }

  if (!(previous == intendedPrevious)){
    LOG(WARNING) << "Update conflict: Request assumes previous revision " <<
        intendedPrevious.getString() << " but latest revision is " <<
        previous.getString();
    return true;
  }
  // register update
  Hash insertedId;
  if (!revision->get("ID", &insertedId)){
    LOG(ERROR) << "Queued history item does not contain ID";
    return true;
  }
  state[item] = &request;
  return false;
}

template<typename Request, typename UpdateState, typename Identifier>
bool Transaction::hasInsertRequestConflictCommons(const Request& request,
                                                  UpdateState& state, Hash& id){
  const typename Request::first_type& table = request.first;
  const SharedRevisionPointer& revision = request.second;
  if (!revision->get("ID", &id)){
    LOG(ERROR) << "Queued request revision does not contain ID";
    return true;
  }
  // Conflict if id present in table
  if (table.rawGetRow(id)){
    LOG(WARNING) << "Table " << table.name() << " already contains id " <<
        id.getString() << ", transaction conflict!";
    return true;
  }
  // Conflict if id present in previous insert requests within same transaction
  Identifier inserted(table, id);
  if (state.find(inserted) != state.end()){
    LOG(WARNING) << "Id conflict for table " << table.name() << ", id " <<
        id.getString() << ", previously inserted in same transaction!";
    return true;
  }
  return false;
}

} /* namespace map_api */
