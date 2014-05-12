#include <map-api/transaction.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <map-api/map-api-core.h>

DECLARE_string(ipPort);

namespace map_api {

std::recursive_mutex Transaction::dbMutex_;

Transaction::Transaction(const Id& owner) : owner_(owner),
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
    std::lock_guard<std::recursive_mutex> lock(dbMutex_);
    // check for conflicts in insert queue
    if (hasMapConflict(insertions_)) {
      LOG(WARNING) << "Insert conflict, commit fails";
      return false;
    }
    if (hasMapConflict(updates_)){
      LOG(WARNING) << "Update conflict, commit fails";
      return false;
    }
    // if no conflicts were found, apply changes, starting from inserts...
    // TODO(tcies) ideally, this should be rollback-able, e.g. by using
    // the SQL built-in transactions
    for (const std::pair<CRItemIdentifier, SharedRevisionPointer> &insertion :
        insertions_){
      const CRTableInterface& table = insertion.first.first;
      const Id& id = insertion.first.second;
      Id idCheck;
      const SharedRevisionPointer &revision = insertion.second;
      CHECK_EQ(revision->get("ID", &idCheck), true) <<
          "Revision to be inserted does not contain ID";
      CHECK(id == idCheck) << "Identifier ID does not match revision ID";
      if (!table.rawInsertQuery(*revision)){
        LOG(ERROR) << "Insertion of " << id.hexString() << " into table " <<
            table.name() << " failed, aborting commit.";
        return false;
      }
    }
    // ...then updates
    for (const std::pair<CRUItemIdentifier, SharedRevisionPointer> &update :
        updates_){
      const CRUTableInterface& table = update.first.first;
      const Id& id = update.first.second;
      const SharedRevisionPointer &newRevision = update.second;
      // 1. Fetch id of latest from CRU table
      SharedRevisionPointer current = table.rawGetRow(id);
      Id latestRevisionId;
      if (!current){
        LOG(FATAL) << "Failed to fetch current CRU table entry for " <<
            id.hexString() << " in table " << table.name();
        return false;
      }
      if (!current->get("latest_revision", &latestRevisionId)){
        LOG(ERROR) << "CRU table entry for " << id.hexString() << " of table "
            << table.name() << " seems not to contain 'latest_revision'";
        return false;
      }
      // 2. Create entry in history
      SharedRevisionPointer rawHistory = table.history_->prepareForInsert(
          *newRevision, latestRevisionId);
      if (!rawHistory){
        LOG(ERROR) << "Failed to create revision for insertion into history for"
            << id.hexString() << " of table " << table.name();
        return false;
      }
      Id nextRevisionId;
      CHECK_EQ(rawHistory->get("ID", &nextRevisionId), true) <<
          "Revision generated for history of " << id.hexString() <<
          " in table " << table.name() << " is missing 'ID'";
      if (!table.history_->rawInsertQuery(*rawHistory)){
        LOG(ERROR) << "Failed to insert history item for " << id.hexString() <<
            " of table " << table.name() << ", commit fails.";
      }
      // 3. Link to entry in history
      if (!table.rawUpdateQuery(id, nextRevisionId)){
        LOG(ERROR) << "Failed to link CRU item " << id.hexString() << " in " <<
            table.name() << " with its latest history item, aborting commit.";
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

template<>
bool Transaction::insert<CRTableInterface>(
    CRTableInterface& table, const Id& id, const SharedRevisionPointer& item){
  if (notifyAbortedOrInactive()){
    return false;
  }
  if (!table.IsInitialized()){
    LOG(ERROR) << "Attempted to insert into uninitialized table";
    return false;
  }
  CHECK(item) << "Passed revision pointer is null";
  Id idHash(Id::random());
  item->set("ID",id);
  item->set("owner",owner_);
  insertions_.insert(InsertMap::value_type(
      CRItemIdentifier(table, id), item));
  return true;
}

template<>
bool Transaction::insert<CRUTableInterface>(
    CRUTableInterface& table, const Id& id, const SharedRevisionPointer& item){
  if (notifyAbortedOrInactive()){
    return false;
  }
  if (!table.IsInitialized()){
    LOG(ERROR) << "Attempted to insert into uninitialized table";
    return false;
  }
  CHECK(item) << "Passed revision pointer is null";
  // 1. Prepare a CRU table entry pointing to nothing
  SharedRevisionPointer insertItem = table.getCRUTemplate();
  insertItem->set("ID", id);
  insertItem->set("owner", owner_);
  insertItem->set("latest_revision", Id()); // invalid hash
  insertions_.insert(InsertMap::value_type(
      CRItemIdentifier(table, id), insertItem));

  // 2. Submit revision to update queue if revision matches structure
  if (!item->structureMatch(*table.getTemplate())){
    LOG(ERROR) << "Revision to be inserted into " << table.name() <<
        " does not match its template structurally";
    insertions_.erase(insertions_.find(CRItemIdentifier(table, id)));
    return false;
  }
  updates_.insert(UpdateMap::value_type(
      CRUItemIdentifier(table, id), item));
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
  Transaction::CRUItemIdentifier item(table, id);
  Transaction::UpdateMap::iterator itemIterator = updates_.find(item);
  if (itemIterator != updates_.end()){
    return itemIterator->second;
  }
  // TODO (tcies) per-item reader lock
  std::lock_guard<std::recursive_mutex> lock(dbMutex_);
  // find bookkeeping row
  SharedRevisionPointer cruRow = table.rawGetRow(id);
  if (!cruRow){
    LOG(ERROR) << "Can't find item " << id.hexString() << " in table " <<
        table.name();
    return SharedRevisionPointer();
  }
  Id latest;
  if (!cruRow->get("latest_revision", &latest)){
    LOG(ERROR) << "Bookkeeping item does not contain reference to latest";
    return SharedRevisionPointer();
  }
  return table.history_->revisionAt(latest, beginTime_);
}

template<>
bool Transaction::dumpTable<CRTableInterface>(CRTableInterface& table,
                 std::vector<SharedRevisionPointer>* dest) {
  // TODO(tcies) incorporate updates pending in transaction (use case?)
  for (const std::pair<CRItemIdentifier,
      const SharedRevisionPointer> &insertion : insertions_) {
    // TODO(tcies) I guess we should filter insertions_ by table
    //   if (insertion.first == table) <-- this is not working, but I guess we
    //   should check which table we want to get data from
    if (true) {
      dest->push_back(insertion.second);
    }
  }

  std::lock_guard<std::recursive_mutex> lock(dbMutex_);
  table.rawDump(dest);

  // TODO(tcies) test
  return true;
}

template<>
bool Transaction::dumpTable<CRUTableInterface>(CRUTableInterface& table,
                 std::vector<SharedRevisionPointer>* dest) {
  if (notifyAbortedOrInactive()){
    return false;
  }

  // fast check in uncommitted transaction queries
  // TODO(tcies) again, not sure how to filter by table here
  for (const std::pair<CRUItemIdentifier, SharedRevisionPointer> &update :
      updates_) {
    if (true) {
      dest->push_back(update.second);
    }
  }

  std::lock_guard<std::recursive_mutex> lock(dbMutex_);
  // find bookkeeping rows in the table
  std::vector<SharedRevisionPointer> cru_rows;
  table.rawDump(&cru_rows);

  for (const SharedRevisionPointer& cru_row : cru_rows) {
    if (!cru_row) {
      // TODO(tcies) not sure how should I characterize the item here
      LOG(ERROR) << "Can't find item " << "ID" << " in table " <<
            table.name();
    }

    Id latest;
    if (!cru_row->get("latest_revision", &latest)) {
      LOG(ERROR) << "Bookkeeping item does not contain reference to latest";
    }

    dest->push_back(table.history_->revisionAt(latest, beginTime_));
  }

  // TODO(tcies) test
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
