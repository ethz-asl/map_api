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

CRUTable::~CRUTable() {}

bool CRUTable::update(Revision* query) {
  CHECK(isInitialized()) << "Attempted to update in non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(reference->structureMatch(*query)) <<
      "Bad structure of update revision";
  Id id;
  query->get(kIdField, &id);
  CHECK_NE(id, Id()) << "Attempted to update element with invalid ID";
  std::shared_ptr<Revision> current = getById(id, Time());
  Time insert_time;
  query->get(kInsertTimeField, &insert_time);
  CHECK(current->verify(kInsertTimeField, insert_time));
  Time previous_time, update_time = Time(); // FIXME(tcies) #66
  query->get(kUpdateTimeField, &previous_time);
  CHECK(previous_time <= update_time);
  query->set(kPreviousTimeField, previous_time);
  query->set(kUpdateTimeField, update_time);
  query->set(kNextTimeField, Time(0));
  CHECK(insertUpdatedCRUDerived(*query));
  CHECK(updateCurrentReferToUpdatedCRUDerived(id, previous_time, update_time));
  return true;
}

bool CRUTable::getLatestUpdateTime(const Id& id, Time* time) {
  CHECK_NE(Id(), id);
  CHECK_NOTNULL(time);
  std::shared_ptr<Revision> row = getById(id, Time());
  ItemDebugInfo itemInfo(name(), id);
  if (!row){
    LOG(ERROR) << itemInfo << "Failed to retrieve row";
    return false;
  }
  row->get(kUpdateTimeField, time);
  return true;
}

const std::string CRUTable::kUpdateTimeField = "update_time";
const std::string CRUTable::kPreviousTimeField = "previous_time";
const std::string CRUTable::kNextTimeField = "next_time";

bool CRUTable::initCRDerived() {
  descriptor_->addField<Time>(kUpdateTimeField);
  descriptor_->addField<Time>(kPreviousTimeField);
  descriptor_->addField<Time>(kNextTimeField);
  initCRUDerived();
  return true;
}

bool CRUTable::insertCRDerived(Revision* query) {
  query->set(kUpdateTimeField, Time());
  query->set(kPreviousTimeField, Time(0));
  query->set(kNextTimeField, Time(0));
  return insertCRUDerived(query);
}

int CRUTable::findByRevisionCRDerived(
    const std::string& key, const Revision& valueHolder, const Time& time,
    std::unordered_map<Id, std::shared_ptr<Revision> >* dest) {
  return findByRevisionCRUDerived(key, valueHolder, time, dest);
}

}
