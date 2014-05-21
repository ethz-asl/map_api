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

const std::string CRUTableInterface::kUpdateTimeField = "update_time";
const std::string CRUTableInterface::kPreviousField = "previous";

bool CRUTableInterface::init() {
  // adding fields that make this an updateable table
  addField<Time>(kUpdateTimeField);
  addField<Id>(kPreviousField);
  history_.reset(new History(name()));
  bool history_initialized = history_->init();
  bool cr_initialized = CRTableInterface::init();
  return history_initialized && cr_initialized;
}

bool CRUTableInterface::rawInsertImpl(Revision& query) const {
  query.set(kUpdateTimeField, Time());
  query.set(kPreviousField, Id());
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
  query.get(kIdField, &id);
  CHECK_NE(id, Id()) << "Attempted to update element with invalid ID";
  return rawUpdateImpl(query);
}

bool CRUTableInterface::rawUpdateImpl(Revision& query) const {
  Id id;
  query.get(kIdField, &id);
  ItemDebugInfo info(name(), id);
  // 1. archive current
  std::shared_ptr<Revision> current = rawGetById(id, Time());
  CHECK(current) << info << "Attempted to update nonexistent item";
  std::shared_ptr<Revision> archive = history_->getTemplate();
  Id archiveId = Id::random();
  archive->set(kIdField, archiveId);
  Id previous;
  query.get(kPreviousField, &previous);
  archive->set(History::kPreviousField, previous);
  Time time;
  query.get(kUpdateTimeField, &time);
  archive->set(History::kRevisionTimeField, time);
  archive->set(History::kRevisionField, *current);
  if (!history_->rawInsert(*archive)) {
    LOG(FATAL) << info << "Failed to insert current version into history";
  }
  // 2. overwrite
  query.set(kUpdateTimeField, Time());
  query.set(kPreviousField, archiveId);
  // Bag for blobs that need to stay in scope until statement is executed
  std::vector<std::shared_ptr<Poco::Data::BLOB> > placeholderBlobs;
  Poco::Data::Statement statement(*session_);
  statement << "UPDATE " << name() << " SET ";
  for (int i = 0; i < query.fieldqueries_size(); ++i) {
    if (i > 0) {
      statement << ", ";
    }
    statement << query.fieldqueries(i).nametype().name() << " = ";
    placeholderBlobs.push_back(query.insertPlaceHolder(i, statement));
  }
  statement << " WHERE " << kIdField << " LIKE ";
  query.insertPlaceHolder(kIdField, statement);

  try {
    statement.execute();
  } catch (const std::exception &e) {
    LOG(FATAL) << "Update failed with exception \"" << e.what() << "\", " <<
        " statement was \"" << statement.toString() << "\" and query :" <<
        query.DebugString();
  }

  return true;
}

bool CRUTableInterface::rawLatestUpdateTime(
    const Id& id, Time* time) const{
  std::shared_ptr<Revision> row = rawGetById(id, Time());
  ItemDebugInfo itemInfo(name(), id);
  if (!row){
    LOG(ERROR) << itemInfo << "Failed to retrieve row";
    return false;
  }
  row->get(kUpdateTimeField, time);
  return true;
}

}
