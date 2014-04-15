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
  // start up core if not running yet TODO(tcies) do this in the core
  if (!MapApiCore::getInstance().isInitialized()) {
    MapApiCore::getInstance().init(FLAGS_ipPort);
  }
  session_ = MapApiCore::getInstance().getSession();
  active_ = true;
  beginTime_ = Time();
  return true;
}

bool Transaction::commit(){
  if (notifyAbortedOrInactive()){
    return false;
  }
  // TODO(tcies) return false if no jobs scheduled
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
  crInsertQueue_.push(CRInsertRequest(table, item));
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
  crInsertQueue_.push(CRInsertRequest(table, insertItem));

  // 2. Prepare history entry and submitting to update queue
  SharedRevisionPointer updateItem =
      table.history_->prepareForInsert(*item, Hash());
  if (!updateItem){
    LOG(ERROR) << "Preparation of insert statement failed for item";
    // aborts transaction TODO(or just abort insert?)
    abort();
    return Hash();
  }
  updateQueue_.push(UpdateRequest(
      ItemIdentifier(table, idHash),
      updateItem));
  // TODO(tcies) register updateItem as latest revision of table:insertItem
  // transaction-internally
  return idHash;
}


// Going with locks for now
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

} /* namespace map_api */
