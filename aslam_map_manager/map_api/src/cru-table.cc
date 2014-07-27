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

DEFINE_bool(cru_linked, false, "Determines whether a revision has references "\
            "to the previous and next revision.");

namespace map_api {

CRUTable::~CRUTable() {}

bool CRUTable::update(Revision* query) {
  CHECK(isInitialized()) << "Attempted to update in non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(reference->structureMatch(*query)) <<
      "Bad structure of update revision";
  Id id;
  query->get(kIdField, &id);
  CHECK_NE(id, Id()) << "Attempted to update element with invalid ID";
  std::shared_ptr<Revision> current = getById(id, LogicalTime::sample());
  LogicalTime insert_time;
  query->get(kInsertTimeField, &insert_time);
  CHECK(current->verify(kInsertTimeField, insert_time));
  LogicalTime previous_time, update_time = LogicalTime::sample();
  current->get(kPreviousTimeField, &previous_time);
  CHECK(previous_time < update_time);
  query->set(kPreviousTimeField, previous_time);
  query->set(kUpdateTimeField, update_time);
  query->set(kNextTimeField, LogicalTime());
  CHECK(insertUpdatedCRUDerived(*query));
  CHECK(updateCurrentReferToUpdatedCRUDerived(id, previous_time, update_time));
  return true;
}

bool CRUTable::getLatestUpdateTime(const Id& id, LogicalTime* time) {
  CHECK_NE(Id(), id);
  CHECK_NOTNULL(time);
  std::shared_ptr<Revision> row = getById(id, LogicalTime::sample());
  ItemDebugInfo itemInfo(name(), id);
  if (!row){
    LOG(ERROR) << itemInfo << "Failed to retrieve row";
    return false;
  }
  row->get(kUpdateTimeField, time);
  return true;
}

CRUTable::Type CRUTable::type() const {
  return Type::CRU;
}

const std::string CRUTable::kUpdateTimeField = "update_time";
const std::string CRUTable::kPreviousTimeField = "previous_time";
const std::string CRUTable::kNextTimeField = "next_time";

bool CRUTable::initCRDerived() {
  descriptor_->addField<LogicalTime>(kUpdateTimeField);
  descriptor_->addField<LogicalTime>(kPreviousTimeField);
  descriptor_->addField<LogicalTime>(kNextTimeField);
  initCRUDerived();
  return true;
}

bool CRUTable::insertCRDerived(Revision* query) {
  query->set(kUpdateTimeField, LogicalTime::sample());
  query->set(kPreviousTimeField, LogicalTime());
  query->set(kNextTimeField, LogicalTime());
  return insertCRUDerived(query);
}

bool CRUTable::bulkInsertCRDerived(const RevisionMap& query) {
  for (const RevisionMap::value_type& item : query) {
    item.second->set(kUpdateTimeField, LogicalTime::sample());
    item.second->set(kPreviousTimeField, LogicalTime());
    item.second->set(kNextTimeField, LogicalTime());
  }
  return bulkInsertCRUDerived(query);
}

int CRUTable::findByRevisionCRDerived(
    const std::string& key, const Revision& valueHolder,
    const LogicalTime& time, RevisionMap* dest) {
  return findByRevisionCRUDerived(key, valueHolder, time, dest);
}

}
