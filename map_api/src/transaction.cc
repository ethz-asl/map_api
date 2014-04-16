/*
 * transaction.cc
 *
 *  Created on: Apr 3, 2014
 *      Author: titus
 */

#include <map-api/transaction.h>

#include <mutex>

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
  if (crInsertQueue_.empty() && cruInsertQueue_.empty() &&
      updateQueue_.empty()){
    LOG(WARNING) << "Committing transaction with no queries";
    return false;
  }
  // Acquire lock for database updates TODO(tcies) per-item locks
  static std::mutex dbMutex;
  {
    std::lock_guard<std::mutex> lock(dbMutex);
    // check for conflicts in all request queues
    if (queueConflict(crInsertQueue_) || queueConflict(cruInsertQueue_) ||
        queueConflict(updateQueue_)){
      LOG(WARNING) << "Conflict in transaction requests queues, commit fails";
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
  Hash idHash = Hash::randomHash();
  item->set("ID",idHash);
  item->set("owner",owner_);
  crInsertQueue_.push_back(CRInsertRequest(table, item));
  return idHash;
}

template<>
Hash Transaction::insert<CRUTableInterface>(
    CRUTableInterface& table,
    const SharedRevisionPointer& item){
  // 1. Prepare a CRU table entry pointing to nothing
  Hash idHash = Hash::randomHash();
  SharedRevisionPointer insertItem = table.getTemplate();
  insertItem->set("ID", idHash);
  insertItem->set("owner", owner_);
  insertItem->set("latest_revision", Hash()); // invalid hash
  cruInsertQueue_.push_back(CRUInsertRequest(table, insertItem));

  // 2. Prepare history entry and submit to update queue
  SharedRevisionPointer updateItem =
      table.history_->prepareForInsert(*item, Hash());
  if (!updateItem){
    LOG(ERROR) << "Preparation of insert statement failed for item";
    // aborts transaction TODO(or just abort insert?)
    abort();
    return Hash();
  }
  updateQueue_.push_back(UpdateRequest(
      ItemIdentifier(table, idHash),
      updateItem));
  // TODO(tcies) register updateItem as latest revision of table:insertItem
  // transaction-internally
  return idHash;
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

template<typename Queue>
bool Transaction::queueConflict(const Queue& queue){
  for (const auto& request : queue){
    if (requestConflict(request)){
      return true;
    }
  }
  return false;
}

/**
 * Insert requests conflict only if the id is already present
 */
template<>
bool Transaction::requestConflict<Transaction::CRInsertRequest>(
    const Transaction::CRInsertRequest& request){
  return this->insertRequestConflict(request);
}
template<>
bool Transaction::requestConflict<Transaction::CRUInsertRequest>(
    const Transaction::CRUInsertRequest& request){
  return this->insertRequestConflict(request);
}
template<typename InsertRequest>
bool Transaction::insertRequestConflict(const InsertRequest& request){
  const auto& table = request.first; // will be CR- or CRUTableInterface
  const SharedRevisionPointer& revision = request.second;
  Hash id;
  if (!revision->get("ID", &id)){
    LOG(ERROR) << "Queued request revision does not contain ID";
    return true;
  }
  if (table.rawGetRow(id)){
    LOG(WARNING) << "Table " << table.name() << " already contains id " <<
        id.getString() << ", transaction conflict!";
    return true;
  }
  return false;
}

/**
 * Update requests conflict if there is a revision that is later than the one
 * referenced as "previous" in the update request.
 */
template<>
bool Transaction::requestConflict<Transaction::UpdateRequest>(
    const Transaction::UpdateRequest& request){
  CRUTableInterface& table = request.first.first;
  const Hash& id = request.first.second;
  const SharedRevisionPointer& revision = request.second;
  SharedRevisionPointer currentRow = table.rawGetRow(id);
  if (!currentRow){
    // TODO(tcies) look in insert queries
    LOG(WARNING) << "Element to be updated seems not to exist";
    return true;
  }
  Hash latestRevision;
  if (!currentRow->get("latest_revision", &latestRevision)){
    LOG(ERROR) << "CRU table " << table.name() << " seems to miss "\
        "'latest_revision' column";
    return true;
  }
  Hash intendedPrevious;
  if (!revision->get("previous", &intendedPrevious)){
    LOG(ERROR) << "Queued history item does not contain reference to previous "\
        "revision";
    return true;
  }
  // TODO(tcies) keep track of previous updates on the same object in the
  // same transaction
  if (!(latestRevision == intendedPrevious)){
    LOG(WARNING) << "Update conflict: Request assumes previous revision " <<
        intendedPrevious.getString() << " but latest revision is " <<
        latestRevision.getString();
    return true;
  }
  return false;
}

} /* namespace map_api */
