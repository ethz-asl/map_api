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
  journal_ = std::stack<JournalEntry>();
  // start up core if not running yet TODO(tcies) do this in the core
  if (!MapApiCore::getInstance().isInitialized()) {
    MapApiCore::getInstance().init(FLAGS_ipPort);
  }
  session_ = MapApiCore::getInstance().getSession();
  active_ = true;
  return true;
}

bool Transaction::commit(){
  if (notifyAbortedOrInactive()){
    return false;
  }
  // unlock all committed rows
  active_ = false;
  return true;
}

bool Transaction::abort(){
  if (notifyAbortedOrInactive()){
    return false;
  }
  // roll back journal
  active_ = false;
  return true;
}

bool Transaction::addInsertQuery(const SharedQueryPointer& query){
  // invalid old state pointer means insert in journal entry
  this->commonOperations(SharedQueryPointer(), query);

  // SQL transaction: Makes insert & locking atomic. We could have all
  // statements of a map_api::Transaction wrapped in an SQL transaction
  // but then we couldn't test the Transaction functionality we will need
  // for a distributed system anyways
  *session_ << "BEGIN TRANSACTION; ", Poco::Data::now;

  // assemble SQLite statement
  Poco::Data::Statement stat(*session_);
  // NB: sqlite placeholders work only for column values
  stat << "INSERT INTO " << query->target() << " ";

  stat << "(";
  for (int i = 0; i < query->fieldqueries_size(); ++i){
    if (i > 0){
      stat << ", ";
    }
    const TableField& field =
        static_cast<const TableField&>(query->fieldqueries(i));
    stat << field.nametype().name();
  }
  stat << ") VALUES ( ";
  for (int i = 0; i < query->fieldqueries_size(); ++i){
    if (i > 0){
      stat << " , ";
    }
    const TableField& field =
        static_cast<const TableField&>(query->fieldqueries(i));
    field.insertPlaceHolder(stat);
  }
  stat << " ); ";

  try {
    stat.execute();
  } catch(std::exception &e){
    LOG(FATAL) << "Insert failed with exception " << e.what();
  }

  *session_ << "COMMIT TRANSACTION; ", Poco::Data::now;

  system("cp database.db ~/temp");

  // TODO (tcies) trigger subscribers
  return true;
}

bool Transaction::addUpdateQuery(const SharedQueryPointer& oldState,
                                 const SharedQueryPointer& newState){
  return true;
}

Transaction::SharedQueryPointer Transaction::addSelectQuery(
    const std::string& table, const Hash& id){
  if (notifyAbortedOrInactive()){
    return SharedQueryPointer();
  }
  return SharedQueryPointer();
}

std::shared_ptr<std::vector<proto::TableFieldDescriptor> >
Transaction::requiredTableFields(){
  std::shared_ptr<std::vector<proto::TableFieldDescriptor> > fields(
      new std::vector<proto::TableFieldDescriptor>);
  fields->push_back(proto::TableFieldDescriptor());
  fields->back().set_name("locked_by");
  fields->back().set_type(proto::TableFieldDescriptor_Type_HASH128);
  return fields;
}

bool Transaction::commonOperations(const SharedQueryPointer& oldState,
                                   const SharedQueryPointer& newState){
  if (notifyAbortedOrInactive()){
    return false;
  }
  (*newState)["locked_by"].set(owner_);
  // check will be done within SQL transaction
  journal_.push(JournalEntry(oldState, newState));
  return true;
}

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
