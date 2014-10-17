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
#include "./core.pb.h"

namespace map_api {

CRUTable::~CRUTable() {}

void CRUTable::update(const std::shared_ptr<Revision>& query) {
  CHECK(query != nullptr);
  update(query, LogicalTime::sample());
}

void CRUTable::update(const std::shared_ptr<Revision>& query,
                      const LogicalTime& time) {
  CHECK(query != nullptr);
  CHECK(isInitialized()) << "Attempted to update in non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  // TODO(tcies) const template, cow template?
  CHECK(query->structureMatch(*reference))
      << "Bad structure of update revision";
  CHECK(query->getId<Id>().isValid())
      << "Attempted to update element with invalid ID";
  LogicalTime update_time = time;
  query->setUpdateTime(update_time);
  CHECK(insertUpdatedCRUDerived(query));
}

bool CRUTable::getLatestUpdateTime(const Id& id, LogicalTime* time) {
  CHECK_NE(Id(), id);
  CHECK_NOTNULL(time);
  std::shared_ptr<const Revision> row = getById(id, LogicalTime::sample());
  ItemDebugInfo itemInfo(name(), id);
  if (!row) {
    LOG(ERROR) << itemInfo << "Failed to retrieve row";
    return false;
  }
  *time = row->getUpdateTime();
  return true;
}

void CRUTable::remove(const LogicalTime& time,
                      const std::shared_ptr<Revision>& query) {
  CHECK(query != nullptr);
  CHECK(isInitialized());
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(query->structureMatch(*reference));
  CHECK_NE(query->getId<Id>(), Id());
  LogicalTime update_time = time;
  query->setUpdateTime(update_time);
  query->setRemoved();
  CHECK(insertUpdatedCRUDerived(query));
}

void CRUTable::findHistoryByRevision(int key, const Revision& valueHolder,
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
const std::string CRUTable::kRemovedField = "removed";

bool CRUTable::insertCRDerived(const LogicalTime& time,
                               const std::shared_ptr<Revision>& query) {
  query->setUpdateTime(time);
  return insertCRUDerived(query);
}

bool CRUTable::bulkInsertCRDerived(const NonConstRevisionMap& query,
                                   const LogicalTime& time) {
  for (const NonConstRevisionMap::value_type& item : query) {
    item.second->setUpdateTime(time);
  }
  return bulkInsertCRUDerived(query);
}

}  // namespace map_api
