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
    // initialize UpdateStates for stateful conflict checking
    CRUpdateState crUpdateState;
    CRUUpdateState cruUpdateState;
    // check for conflicts in insert queues
    if (queueConflict(crInsertQueue_, crUpdateState) ||
        queueConflict(cruInsertQueue_, cruUpdateState)){
      LOG(WARNING) << "Conflict in insert request queues, commit fails";
      return false;
    }
    // check for conflicts in update queus, this needs to come after the insert
    // queues due to stateful conflict checking: We need to take into account
    // updates that are performed on items inserted in the same transaction
    if (queueConflict(updateQueue_, cruUpdateState)){
      LOG(WARNING) << "Conflict in update request queue, commit fails";
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
      CRUItemIdentifier(table, idHash),
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

template<typename Queue, typename UpdateState>
bool Transaction::queueConflict(const Queue& queue, UpdateState& state){
  for (const auto& request : queue){
    if (requestConflict(request, state)){
      return true;
    }
  }
  return false;
}

/**
 * CR insert requests conflict only if the id is already present
 */
template<>
bool Transaction::requestConflict<Transaction::CRInsertRequest,
Transaction::CRUpdateState>(const Transaction::CRInsertRequest& request,
                            Transaction::CRUpdateState& state){
  Hash id;
  if (insertRequestConflictCommons<Transaction::CRInsertRequest,
      Transaction::CRUpdateState, Transaction::CRItemIdentifier>(request,
                                                                 state, id)){
    return true;
  }
  // Register insert for stateful conflict checking
  Transaction::CRItemIdentifier inserted(request.first, id);
  state.insert(inserted);
  return false;
}
/**
 * CRU insert request: very similar to CR insert request
 */
template<>
bool Transaction::requestConflict<Transaction::CRUInsertRequest,
Transaction::CRUUpdateState>(const Transaction::CRUInsertRequest& request,
                             Transaction::CRUUpdateState& state){
  Hash id;
  if (insertRequestConflictCommons<Transaction::CRUInsertRequest,
      Transaction::CRUUpdateState, Transaction::CRUItemIdentifier>(request,
                                                                 state, id)){
    return true;
  }
  // Register insert for stateful conflict checking
  Transaction::CRUItemIdentifier inserted(request.first, id);
  state[inserted] = Hash();
  return false;
}

/**
 * Update requests conflict if there is a revision that is later than the one
 * referenced as "previous" in the update request.
 */
template<>
bool Transaction::requestConflict<Transaction::UpdateRequest,
Transaction::CRUUpdateState>(const Transaction::UpdateRequest& request,
                             Transaction::CRUUpdateState& state){
  const CRUItemIdentifier& item = request.first;
  const SharedRevisionPointer& revision = request.second;
  // if the item to be updated is not yet in the update state cache, fetch it
  if (state.find(item) == state.end()){
    const CRUTableInterface& table = item.first;
    const Hash& id = item.second;
    SharedRevisionPointer currentRow = table.rawGetRow(id);
    if (!currentRow){
      LOG(WARNING) << "Element to be updated seems not to exist";
      return true;
    }
    Hash latestRevision;
    if (!currentRow->get("latest_revision", &latestRevision)){
      LOG(ERROR) << "CRU table " << table.name() << " seems to miss "\
          "'latest_revision' column";
      return true;
    }
    state[item] = latestRevision;
  }
  // compare it to the "previous" element inteded by the udpate reqest
  Hash intendedPrevious;
  if (!revision->get("previous", &intendedPrevious)){
    LOG(ERROR) << "Queued history item does not contain reference to previous "\
        "revision";
    return true;
  }
  if (!(state[item] == intendedPrevious)){
    LOG(WARNING) << "Update conflict: Request assumes previous revision " <<
        intendedPrevious.getString() << " but latest revision is " <<
        state[item].getString();
    return true;
  }
  // register update
  Hash insertedId;
  if (!revision->get("ID", &insertedId)){
    LOG(ERROR) << "Queued history item does not contain ID";
    return true;
  }
  state[item] = insertedId;
  return false;
}

template<typename Request, typename UpdateState, typename Identifier>
bool Transaction::insertRequestConflictCommons(const Request& request,
                                               UpdateState& state, Hash& id){
  const auto& table = request.first;
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
