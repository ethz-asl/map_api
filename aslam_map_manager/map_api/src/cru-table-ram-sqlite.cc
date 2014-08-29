#include "map-api/cru-table-ram-sqlite.h"

#include "map-api/core.h"

namespace map_api {

CRUTableRamSqlite::~CRUTableRamSqlite() {}

bool CRUTableRamSqlite::initCRUDerived() {
  sqlite_interface_.init(Core::getSession());
  CHECK(sqlite_interface_.isSqlSafe(*descriptor_));
  CHECK(sqlite_interface_.create(*descriptor_));
  return true;
}

bool CRUTableRamSqlite::insertCRUDerived(Revision* query) {
  return sqlite_interface_.insert(*query);
}

bool CRUTableRamSqlite::bulkInsertCRUDerived(const RevisionMap& query) {
  return sqlite_interface_.bulkInsert(query);
}

bool CRUTableRamSqlite::patchCRDerived(const Revision& query) {
  return sqlite_interface_.insert(query);
}

int CRUTableRamSqlite::findByRevisionCRUDerived(const std::string& key,
                                                const Revision& value_holder,
                                                const LogicalTime& time,
                                                RevisionMap* dest) {
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
  if (key != "") {
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
    Id id;
    item->get(kIdField, &id);
    CHECK(id.isValid());
    std::unordered_map<Id, LogicalTime> latest;
    LogicalTime item_time;
    item->get(kUpdateTimeField, &item_time);
    if (item_time > latest[id]) {
      (*dest)[id] = item;
      latest[id] = item_time;
    }
  }
  return dest->size();
}

int CRUTableRamSqlite::countByRevisionCRUDerived(const std::string& key,
                                                 const Revision& value_holder,
                                                 const LogicalTime& time) {
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
  if (key != "") {
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

bool CRUTableRamSqlite::insertUpdatedCRUDerived(const Revision& query) {
  sqlite_interface_.insert(query);
  return true;
}

void CRUTableRamSqlite::findHistoryByRevisionCRUDerived(
    const std::string& /*key*/, const Revision& /*valueHolder*/,
    const LogicalTime& /*time*/, HistoryMap* /*dest*/) {
  // TODO(tcies) implement
  CHECK(false) << "Remains to be implemented";
}

} /* namespace map_api */
