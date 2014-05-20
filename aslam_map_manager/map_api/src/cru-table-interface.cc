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
  addField<Time>("update_time");
  addField<Id>("previous"); // id of previous revision in history table
  history_.reset(new History(name()));
  return history_->init() && CRTableInterface::init();
}

bool CRUTableInterface::rawInsertImpl(Revision& query) const {
  query.set("update_time", Time());
  query.set("previous", Id());
  return CRTableInterface::rawInsertImpl(query);
}

int CRUTableInterface::rawFindByRevisionImpl(
      const std::string& key, const Revision& valueHolder, const Time& time,
      std::vector<std::shared_ptr<Revision> >* dest)  const {
  // for now, no difference from CRTableInterface - see documentation at
  // declaration
  return CRTableInterface::rawFindByRevisionImpl(key, valueHolder, time, dest);
}

bool CRUTableInterface::rawUpdate(Revision& query) const {
  CHECK(isInitialized()) << "Attempted to update in non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(reference->structureMatch(query)) << "Bad structure of update revision";
  Id id;
  query.get("ID", &id);
  CHECK_NE(id, Id()) << "Attempted to update element with invalid ID";
  return rawUpdateImpl(query);
}

bool CRUTableInterface::rawUpdateImpl(Revision& query) const {
  Id id;
  query.get("ID", &id);
  ItemDebugInfo info(name(), id);
  // 1. archive current
  std::shared_ptr<Revision> current = rawGetById(id, Time());
  CHECK(current) << info << "Attempted to update nonexistent item";
  std::shared_ptr<Revision> archive = history_->getTemplate();
  Id archiveId = Id::random();
  archive->set("ID", archiveId);
  Id previous;
  query.get("previous", &previous);
  archive->set("previous", previous);
  Time time;
  query.get("update_time", &time);
  archive->set("revision_time", time);
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
  query.set("update_time", Time());
  query.set("previous", archiveId);
  // important: Needs to call CR implementation, not CRU implementation through
  // non-virtual interface rawInsert(), to keep correct 'previous' field value
  // AND correct create_time field value
  return CRTableInterface::rawInsertImpl(query);
}

bool CRUTableInterface::rawLatestUpdateTime(
    const Id& id, Time* time) const{
  std::shared_ptr<Revision> row = rawGetById(id, Time());
  ItemDebugInfo itemInfo(name(), id);
  if (!row){
    LOG(ERROR) << itemInfo << "Failed to retrieve row";
    return false;
  }
  row->get("update_time", time);
  return true;
}

}
