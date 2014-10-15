#include "map-api/cru-table-ram-sqlite.h"

#include "map-api/core.h"

namespace map_api {

CRUTableRamSqlite::~CRUTableRamSqlite() {}

bool CRUTableRamSqlite::initCRDerived() {
  sqlite_interface_.init(name(), Core::getSession());
  CHECK(sqlite_interface_.isSqlSafe(*descriptor_));
  CHECK(sqlite_interface_.create(*descriptor_));
  return true;
}

bool CRUTableRamSqlite::insertCRUDerived(
    const std::shared_ptr<Revision>& query) {
  return sqlite_interface_.insert(*query);
}

bool CRUTableRamSqlite::bulkInsertCRUDerived(const NonConstRevisionMap& query) {
  return sqlite_interface_.bulkInsert(query);
}

bool CRUTableRamSqlite::patchCRDerived(const std::shared_ptr<Revision>& query) {
  return sqlite_interface_.insert(*query);
}

std::shared_ptr<const Revision> CRUTableRamSqlite::getByIdCRDerived(
    const Id& /*id*/, const LogicalTime& /*time*/) const {
  LOG(FATAL) << "Not implemented";  // TODO(tcies) implement
}

void __attribute__((deprecated)) CRUTableRamSqlite::dumpChunkCRDerived(
    const Id& /*chunk_id*/, const LogicalTime& /*time*/,
    RevisionMap* /*dest*/) {
  LOG(FATAL) << "Not implemented";  // TODO(tcies) implement
}

void __attribute__((deprecated)) CRUTableRamSqlite::findByRevisionCRDerived(
    int key, const Revision& value_holder, const LogicalTime& time,
    RevisionMap* dest) {
  // TODO(tcies) adapt to "removed" flag and int key
  CHECK(false) << "Not adapted to \"removed\" flag and int key!";
  // TODO(tcies) apart from the more sophisticated time query, this is very
  // similar to its CR equivalent. Maybe refactor at some time?
  SqliteInterface::PocoToProto poco_to_proto(getTemplate());
  std::shared_ptr<Poco::Data::Session> session =
      sqlite_interface_.getSession().lock();
  CHECK(session) << "Couldn't lock session weak pointer";
  Poco::Data::Statement statement(*session);
  // caching of data needed for Poco::Data to work
  Poco::UInt64 serialized_time = time.serialize();
  std::vector<std::shared_ptr<Poco::Data::BLOB> > data_holder;
  // TODO(tcies) evt. optimizations from http://www.sqlite.org/queryplanner.html
  statement << "SELECT ";
  poco_to_proto.into(&statement);
  statement << " FROM " << name() << " WHERE " << kUpdateTimeField << " <  ? ",
      Poco::Data::use(serialized_time);
  if (key >= 0) {
    statement << " AND " << key << " LIKE ";
    data_holder.push_back(value_holder.insertPlaceHolder(key, &statement));
  }
  try {
    statement.execute();
  }
  catch (const std::exception& e) {  // NOLINT
    LOG(FATAL) << "Find statement failed: " << statement.toString()
               << " with exception: " << e.what();
  }
  std::vector<std::shared_ptr<Revision> > from_poco;
  poco_to_proto.toProto(&from_poco);
  for (const std::shared_ptr<Revision>& item : from_poco) {
    Id id = item->getId<Id>();
    CHECK(id.isValid());
    std::unordered_map<Id, LogicalTime> latest;
    LogicalTime item_time = item->getUpdateTime();
    if (item_time > latest[id]) {
      (*dest)[id] = item;
      latest[id] = item_time;
    }
  }
}

void __attribute__((deprecated)) CRUTableRamSqlite::getAvailableIdsCRDerived(
    const LogicalTime& /*time*/, std::unordered_set<Id>* /*ids*/) {
  LOG(FATAL) << "Needs implementation";
}

int __attribute__((deprecated)) CRUTableRamSqlite::countByRevisionCRDerived(
    int key, const Revision& value_holder, const LogicalTime& time) {
  // TODO(tcies) adapt to "removed" flag and int key
  CHECK(false) << "Not adapted to \"removed\" flag and int key!";
  // TODO(tcies) apart from the more sophisticated time query, this is very
  // similar to its CR equivalent. Maybe refactor at some time?
  SqliteInterface::PocoToProto poco_to_proto(getTemplate());
  std::shared_ptr<Poco::Data::Session> session =
      sqlite_interface_.getSession().lock();
  CHECK(session) << "Couldn't lock session weak pointer";
  Poco::Data::Statement statement(*session);
  // Hold on to data for Poco.
  Poco::UInt64 serialized_time = time.serialize();
  std::vector<std::shared_ptr<Poco::Data::BLOB> > data_holder;
  int count = 0;
  statement << "SELECT COUNT(DISTINCT " << kIdField << ")",
      Poco::Data::into(count);
  statement << " FROM " << name() << " WHERE " << kUpdateTimeField << " <  ? ",
      Poco::Data::use(serialized_time);
  if (key >= 0) {
    statement << " AND " << key << " LIKE ";
    data_holder.push_back(value_holder.insertPlaceHolder(key, &statement));
  }
  try {
    statement.execute();
  }
  catch (const std::exception& e) {  // NOLINT
    LOG(FATAL) << "Find statement failed: " << statement.toString()
               << " with exception: " << e.what();
  }
  return count;
}

int __attribute__((deprecated)) CRUTableRamSqlite::countByChunkCRDerived(
    const Id& /*chunk_id*/, const LogicalTime& /*time*/) {
  // TODO(tcies) implement
  CHECK(false) << "Not implemented";
}

bool CRUTableRamSqlite::insertUpdatedCRUDerived(
    const std::shared_ptr<Revision>& query) {
  sqlite_interface_.insert(*query);
  return true;
}

void __attribute__((deprecated))
    CRUTableRamSqlite::findHistoryByRevisionCRUDerived(
        int /*key*/, const Revision& /*valueHolder*/,
        const LogicalTime& /*time*/, HistoryMap* /*dest*/) {
  // TODO(tcies) implement
  CHECK(false) << "Remains to be implemented";
}

void __attribute__((deprecated)) CRUTableRamSqlite::chunkHistory(
    const Id& /*chunk_id*/, const LogicalTime& /*time*/, HistoryMap* /*dest*/) {
  // TODO(tcies) implement
  CHECK(false) << "Remains to be implemented";
}

void __attribute__((deprecated)) CRUTableRamSqlite::itemHistoryCRUDerived(
    const Id& /*id*/, const LogicalTime& /*time*/, History* /*dest*/) {
  // TODO(tcies) implement
  CHECK(false) << "Remains to be implemented";
}

} /* namespace map_api */
