#include "map-api/cru-table.h"

#include <cstdio>
#include <map>

#include <Poco/Data/Common.h>
#include <Poco/Data/Statement.h>
#include <Poco/Data/SQLite/Connector.h>
#include <Poco/Data/BLOB.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "map-api/map-api-core.h"
#include "map-api/local-transaction.h"
#include "core.pb.h"

namespace map_api {

const std::string CRUTable::kUpdateTimeField = "update_time";
const std::string CRUTable::kPreviousTimeField = "previous_time";
const std::string CRUTable::kNextTimeField = "next_time";

CRUTable::~CRUTable() {}

void CRUTable::defineFieldsCRDerived() {
  addField<Time>(kUpdateTimeField);
  addField<Time>(kPreviousTimeField);
  addField<Time>(kNextTimeField);
  defineFieldsCRUDerived();
}

bool CRUTable::rawInsertImpl(Revision* query) const {
  query->set(kUpdateTimeField, Time());
  query->set(kPreviousTimeField, Time(0));
  query->set(kNextTimeField, Time(0));
  return CRTable::rawInsertImpl(query);
}

int CRUTable::rawFindByRevisionImpl(
    const std::string& key, const Revision& valueHolder, const Time& time,
    std::unordered_map<Id, std::shared_ptr<Revision> >* dest)  const {
  // TODO(tcies) apart from the more sophisticated time query, this is very
  // similar to its CR equivalent. Maybe refactor at some time?
  PocoToProto poco_to_proto(*this);
  std::shared_ptr<Poco::Data::Session> session = session_.lock();
  CHECK(session) << "Couldn't lock session weak pointer";
  Poco::Data::Statement statement(*session);
  // TODO(tcies) evt. optimizations from http://www.sqlite.org/queryplanner.html
  statement << "SELECT ";
  poco_to_proto.into(statement);
  statement << " FROM " << name() << " WHERE " << kUpdateTimeField << " <  ? ",
      Poco::Data::use(time.serialize());
  statement << " AND (" << kNextTimeField << " = 0 OR " << kNextTimeField <<
      " > ? ", Poco::Data::use(time.serialize());
  statement << ") ";
  if (key != "") {
    statement << " AND " << key << " LIKE ";
    valueHolder.insertPlaceHolder(key, statement);
  }
  try{
    statement.execute();
  } catch (const std::exception& e){
    LOG(FATAL) << "Find statement failed: " << statement.toString() <<
        " with exception: " << e.what();
  }
  std::vector<std::shared_ptr<Revision> > from_poco;
  poco_to_proto.toProto(&from_poco);
  for (const std::shared_ptr<Revision>& item : from_poco) {
    Id id;
    item->get(kIdField, &id);
    CHECK(id.isValid());
    (*dest)[id] = item;
  }
  return from_poco.size();
}

bool CRUTable::rawUpdate(Revision* query) const {
  CHECK(isInitialized()) << "Attempted to update in non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(reference->structureMatch(*query)) <<
      "Bad structure of update revision";
  Id id;
  query->get(kIdField, &id);
  CHECK_NE(id, Id()) << "Attempted to update element with invalid ID";
  return rawUpdateImpl(query);
}

bool CRUTable::rawUpdateImpl(Revision* query) const {
  Id id;
  query->get(kIdField, &id);
  ItemDebugInfo info(name(), id);
  // 1. Check consistency of insert time field
  // TODO(tcies) generalize this test by introducing a "const" trait to fields?
  std::shared_ptr<Revision> current = rawGetById(id, Time());
  Time query_insert_time;
  query->get(CRTable::kInsertTimeField, &query_insert_time);
  CHECK(current->verify(CRTable::kInsertTimeField, query_insert_time))
  << " Insert time needs to remain same after update.";
  // 2. Define update time, fetch previous time
  Time update_time, previous_time;
  current->get(kUpdateTimeField, &previous_time);
  // 3. Write update time into "next_time" field of current revision
  std::shared_ptr<Poco::Data::Session> session = session_.lock();
  CHECK(session) << "Couldn't lock session weak pointer";
  Poco::Data::Statement statement(*session);
  statement << "UPDATE " << name() << " SET " << kNextTimeField << " = ? ",
      Poco::Data::use(update_time.serialize());
  statement << " WHERE ID = ";
  query->insertPlaceHolder(CRTable::kIdField, statement);
  statement << " AND " << kUpdateTimeField << " = ? ",
      Poco::Data::use(previous_time.serialize());
  try {
    statement.execute();
  } catch (const std::exception& e) {
    LOG(FATAL) << info << kNextTimeField << " update failed with exception \""
        << e.what() << "\", " << " statement was \"" << statement.toString() <<
        "\" and query :" << query->DebugString();
  }
  // 4. Insert updated row
  query->set(kUpdateTimeField, update_time);
  query->set(kPreviousTimeField, previous_time);
  query->set(kNextTimeField, Time(0));
  // calling insert implementation of CR to avoid these fields to be overwritten
  CRTable::rawInsertImpl(query);
  return true;
}

bool CRUTable::rawLatestUpdateTime(
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
