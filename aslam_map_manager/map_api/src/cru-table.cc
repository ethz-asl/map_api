#include "map-api/cru-table.h"

#include <cstdio>
#include <map>

#include <Poco/Data/Common.h>
#include <Poco/Data/Statement.h>
#include <Poco/Data/SQLite/Connector.h>
#include <Poco/Data/BLOB.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "map-api/core.h"
#include "map-api/local-transaction.h"
#include "./core.pb.h"

namespace map_api {

CRUTable::~CRUTable() {}

bool CRUTable::update(Revision* query) {
  update(CHECK_NOTNULL(query), LogicalTime::sample());
  return true;  // TODO(tcies) void
}

void CRUTable::update(Revision* query, const LogicalTime& time) {
  CHECK_NOTNULL(query);
  CHECK(isInitialized()) << "Attempted to update in non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(reference->structureMatch(*query)) <<
      "Bad structure of update revision";
  Id id;
  query->get(kIdField, &id);
  CHECK_NE(id, Id()) << "Attempted to update element with invalid ID";
  LogicalTime update_time = time, previous_time;
  query->set(kUpdateTimeField, update_time);
  CHECK(insertUpdatedCRUDerived(*query));
}

bool CRUTable::getLatestUpdateTime(const Id& id, LogicalTime* time) {
  CHECK_NE(Id(), id);
  CHECK_NOTNULL(time);
  std::shared_ptr<Revision> row = getById(id, LogicalTime::sample());
  ItemDebugInfo itemInfo(name(), id);
  if (!row) {
    LOG(ERROR) << itemInfo << "Failed to retrieve row";
    return false;
  }
  row->get(kUpdateTimeField, time);
  return true;
}

void CRUTable::findHistoryByRevision(const std::string& key,
                                     const Revision& valueHolder,
                                     const LogicalTime& time,
                                     HistoryMap* dest) {
  CHECK(isInitialized()) << "Attempted to find in non-initialized table";
  CHECK_NOTNULL(dest);
  dest->clear();
  CHECK(time < LogicalTime::sample());
  return findHistoryByRevisionCRUDerived(key, valueHolder, time, dest);
}

CRUTable::Type CRUTable::type() const {
  return Type::CRU;
}

const std::string CRUTable::kUpdateTimeField = "update_time";
const std::string CRUTable::kPreviousTimeField = "previous_time";
const std::string CRUTable::kNextTimeField = "next_time";

bool CRUTable::initCRDerived() {
  descriptor_->addField<LogicalTime>(kUpdateTimeField);
  initCRUDerived();
  return true;
}

bool CRUTable::insertCRDerived(Revision* query) {
  query->set(kUpdateTimeField, LogicalTime::sample());
  return insertCRUDerived(query);
}

bool CRUTable::bulkInsertCRDerived(const RevisionMap& query) {
  for (const RevisionMap::value_type& item : query) {
    item.second->set(kUpdateTimeField, LogicalTime::sample());
  }
  return bulkInsertCRUDerived(query);
}

int CRUTable::findByRevisionCRDerived(
    const std::string& key, const Revision& valueHolder,
    const LogicalTime& time, RevisionMap* dest) {
  return findByRevisionCRUDerived(key, valueHolder, time, dest);
}

int CRUTable::countByRevisionCRDerived(const std::string& key,
                                       const Revision& valueHolder,
                                       const LogicalTime& time) {
  return countByRevisionCRUDerived(key, valueHolder, time);
}

CRUTable::History::const_iterator CRUTable::History::latestAt(
    const LogicalTime& time) const {
  return latestAt(time, cbegin()->indexOf(kUpdateTimeField));
}

CRUTable::History::const_iterator CRUTable::History::latestAt(
    const LogicalTime& time, int index_guess) const {
  LogicalTime item_time;
  for (const_iterator it = cbegin(); it != cend(); ++it) {
    it->get(kUpdateTimeField, index_guess, &item_time);
    if (item_time <= time) {
      return it;
    }
  }
  return cend();
}

}  // namespace map_api
