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
  history_.reset(new History(name()));
  return history_->init() && CRTableInterface::init();
}

bool CRUTableInterface::rawInsertImpl(Revision& query) const {
  query.set("time", Time());
  query.set("previous", Id());
  return CRTableInterface::rawInsertImpl(query);
}

std::shared_ptr<Revision> CRUTableInterface::rawGetRowAtTime(
    const Id& id, const Time& time) const {
  // TODO(tcies) this could be SQL optimized by pre-fetching a couple of
  // revisions atime, using the LIMIT statement and ordered be time, descending
  std::shared_ptr<Revision> returnValue = rawGetRow(id);
  Time timeIteration;
  for (returnValue->get("time", &timeIteration); timeIteration > time;
      returnValue->get("time", &timeIteration)) {
    Id previous;
    returnValue->get("previous", &previous);
    std::shared_ptr<Revision> archived = history_->rawGetRow(previous);
    archived->get("revision", returnValue.get());
  }
  return returnValue;
}

void CRUTableInterface::rawDumpAtTime(
    const Time& time, std::vector<std::shared_ptr<Revision> >* dest) const {
  PocoToProto pocoToProto(*this);
  Poco::Data::Statement statement(*session_);
  statement << "SELECT";
  pocoToProto.into(statement);
  statement << "FROM " << name();
  int64_t serializedTime = time.serialize();
  statement << " WHERE time < ?", Poco::Data::use(serializedTime) ;
  try{
    statement.execute();
  } catch (const std::exception& e){
    LOG(FATAL) << "Statement failed: " << statement.toString() <<
        " with exception " << e.what();
  }
  pocoToProto.toProto(dest);
}

bool CRUTableInterface::rawUpdateQuery(Revision& query) const{
  Id id;
  query.get("ID", &id);
  ItemDebugInfo info(name(), id);
  // 1. archive current
  std::shared_ptr<Revision> current = rawGetRow(id);
  CHECK(current) << info << "Attempted to update nonexistent item";
  std::shared_ptr<Revision> archive = history_->getTemplate();
  Id archiveId = Id::random();
  archive->set("ID", archiveId);
  Id previous;
  query.get("previous", &previous);
  archive->set("previous", previous);
  Time time;
  query.get("time", &time);
  archive->set("time", time);
  archive->set("revision", *current);
  if (!history_->rawInsert(*archive)) {
    LOG(FATAL) << info << "Failed to insert current version into history";
  }
  // 2. overwrite
  // TODO(tcies) this is the lazy way, to get it to work; use UPDATE query
  try {
    *session_ << "DELETE FROM " << name() << " WHERE ID LIKE ? ",
        Poco::Data::use(id.hexString()), Poco::Data::now;
  } catch (const std::exception& e) {
    LOG(FATAL) << info << "Delete in update failed with exception " << e.what();
  }
  query.set("time", Time());
  query.set("previous", archiveId);
  // important: Needs to call CR implementation, not CRU implementation through
  // non-virtual interface rawInsert(), to keep correct 'previous' field value
  return CRTableInterface::rawInsertImpl(query);
}

bool CRUTableInterface::rawLatestUpdateTime(
    const Id& id, Time* time) const{
  std::shared_ptr<Revision> row = rawGetRow(id);
  ItemDebugInfo itemInfo(name(), id);
  if (!row){
    LOG(ERROR) << itemInfo << "Failed to retrieve row";
    return false;
  }
  row->get("time", time);
  return true;
}

}
