#include "map-api/cru-table-interface.h"

#include <cstdio>
#include <map>

#include <Poco/Data/Common.h>
#include <Poco/Data/Statement.h>
#include <Poco/Data/SQLite/Connector.h>
#include <Poco/Data/BLOB.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "map-api/map-api-core.h"
#include "map-api/transaction.h"
#include "core.pb.h"

namespace map_api {

CRUTableInterface::~CRUTableInterface() {}

bool CRUTableInterface::init() {
  // adding fields that make this an updateable table
  addField<Time>("time"); // time of update
  addField<Id>("previous"); // id of previous revision in history table
  return history_.init() && CRTableInterface::init();
}

bool CRUTableInterface::rawInsertQuery(Revision& query) const {
  query.set("time", Time());
  query.set("previous", Id());
  return CRTableInterface::rawInsertQuery(query);
}

bool CRUTableInterface::rawUpdateQuery(Revision& query) const{
  Id id;
  query.get("ID", &id);
  ItemDebugInfo info(name(), id);
  // 1. archive current
  std::shared_ptr<Revision> current = rawGetRow(id);
  CHECK(current) << info << "Attempted to update nonexistent item";
  std::shared_ptr<Revision> archive = history_.getTemplate();
  Id archiveId = Id::random();
  archive->set("ID", archiveId);
  Id previous;
  query.get("previous", &previous);
  archive->set("previous", previous);
  Time time;
  query.get("time", &time);
  archive->set("time", time);
  archive->set("revision", current);
  if (!history_.rawInsertQuery(*archive)) {
    LOG(ERROR) << info << "Failed to insert current version into history";
  }
  // 2. overwrite
  // TODO(tcies) this is the lazy way, to get it to work; use UPDATE query
  try {
    *session_ << "DELETE FROM " << name() << " WHERE 'ID' LIKE ?;",
          id.hexString(), Poco::Data::now;
  } catch (const std::exception& e) {
    LOG(ERROR) << info << "Delete in update failed with exception " << e.what();
    return false;
  }
  query.set("time", Time());
  query.set("previous", archiveId);
  return CRTableInterface::rawInsertQuery(query);
}

bool CRUTableInterface::rawLatestUpdate(const Id& id, Time* time) const{
//  std::shared_ptr<Revision> row = rawGetRow(id);
//  ItemDebugInfo itemInfo(name(), id);
//  if (!row){
//    LOG(ERROR) << itemInfo << "Failed to retrieve row";
//    return false;
//  }
//  Id latestInHistoryId;
//  if (!row->get("latest_revision", &latestInHistoryId)){
//    LOG(ERROR) << itemInfo << "Does not contain 'latest_revision'";
//    return false;
//  }
//  std::shared_ptr<Revision> latestInHistory(
//      history_.rawGetRow(latestInHistoryId));
//  if (!latestInHistory){
//    LOG(ERROR) << itemInfo << "Failed to get latest revision in history";
//    return false;
//  }
//  if (!latestInHistory->get("time", time)){
//    LOG(ERROR) << itemInfo << "Latest revision does not contain 'time'";
//    return false;
//  }
  // TODO(tcies) adapt
  return true;
}

}
