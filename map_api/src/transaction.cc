/*
 * transaction.cc
 *
 *  Created on: Apr 3, 2014
 *      Author: titus
 */

#include <map-api/transaction.h>

#include <gflags/gflags.h>

#include <map-api/map-api-core.h>

DECLARE_string(ipPort);

namespace map_api {

Transaction::Transaction(const Hash& owner) : owner_(owner){
}

bool Transaction::begin(){
  journal_ = std::stack<JournalEntry>();
  // start up core if not running yet TODO(tcies) do this in the core
  if (!MapApiCore::getInstance().isInitialized()) {
    MapApiCore::getInstance().init(FLAGS_ipPort);
  }
  session_ = MapApiCore::getInstance().getSession();
  return true;
}

bool Transaction::commit(){
  // unlock all committed rows
  return true;
}

bool Transaction::abort(){
  // roll back journal
  return true;
}

bool Transaction::addInsertQuery(
    std::shared_ptr<const TableInsertQuery> query){
  return true;
}

bool Transaction::addUpdateQuery(
    std::shared_ptr<const TableInsertQuery> groundState,
    std::shared_ptr<const TableInsertQuery> update){
  return true;
}

std::shared_ptr<TableInsertQuery> Transaction::addSelectQuery(
    const std::string& table, const Hash& id){
  return std::shared_ptr<TableInsertQuery>();
}

std::shared_ptr<std::vector<std::string> >
Transaction::requiredTableFields(){
  std::shared_ptr<std::vector<std::string> > fields =
      std::shared_ptr<std::vector<std::string> >(
          new std::vector<std::string>);
  fields->push_back("locked_by");
  return fields;
}

} /* namespace map_api */
