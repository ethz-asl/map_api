#include "map-api/cr-table-ram-cache.h"

#include <glog/logging.h>

#include "map-api/core.h"

namespace map_api {

CRTableRAMCache::~CRTableRAMCache() {}

bool CRTableRAMCache::initCRDerived() {
  sqlite_interface_.init(Core::getSession());
  CHECK(sqlite_interface_.isSqlSafe(*descriptor_));
  CHECK(sqlite_interface_.create(*descriptor_));
  return true;
}

bool CRTableRAMCache::insertCRDerived(Revision* query) {
  return sqlite_interface_.insert(*query);
}

bool CRTableRAMCache::bulkInsertCRDerived(const RevisionMap& query) {
  return sqlite_interface_.bulkInsert(query);
}

bool CRTableRAMCache::patchCRDerived(const Revision& query) {
  return sqlite_interface_.insert(query);
}

int CRTableRAMCache::findByRevisionCRDerived(
    const std::string& key, const Revision& value_holder,
    const LogicalTime& time, RevisionMap* dest) {
  SqliteInterface::PocoToProto pocoToProto(getTemplate());
  std::shared_ptr<Poco::Data::Session> session =
      sqlite_interface_.getSession().lock();
  CHECK(session) << "Couldn't lock session weak pointer";
  Poco::Data::Statement statement(*session);
  // need to cache data for Poco
  uint64_t serialized_time = time.serialize();
  std::vector<std::shared_ptr<Poco::Data::BLOB> > data_holder;
  statement << "SELECT";
  pocoToProto.into(statement);
  statement << "FROM " << name() << " WHERE " << kInsertTimeField << " <= ? ",
      Poco::Data::use(serialized_time);
  if (key != "") {
    statement << " AND " << key << " LIKE ";
    data_holder.push_back(value_holder.insertPlaceHolder(key, statement));
  }
  try {
    statement.execute();
  }
  catch (const std::exception& e) {
    LOG(FATAL) << "Find statement failed: " << statement.toString() <<
        " with exception: " << e.what();
  }
  std::vector<std::shared_ptr<Revision> > from_poco;
  pocoToProto.toProto(&from_poco);
  for (const std::shared_ptr<Revision>& item : from_poco) {
    Id id;
    item->get(kIdField, &id);
    CHECK(id.isValid());
    (*dest)[id] = item;
  }
  return from_poco.size();
}

int CRTableRAMCache::countByRevisionCRDerived(const std::string& key,
                                              const Revision& value_holder,
                                              const LogicalTime& time) {
  SqliteInterface::PocoToProto pocoToProto(getTemplate());
  std::shared_ptr<Poco::Data::Session> session =
      sqlite_interface_.getSession().lock();
  CHECK(session) << "Couldn't lock session weak pointer";
  Poco::Data::Statement statement(*session);
  // Hold on to data for Poco.
  uint64_t serialized_time = time.serialize();
  std::vector<std::shared_ptr<Poco::Data::BLOB> > data_holder;
  int count = 0;
  statement << "SELECT COUNT(" << kIdField << ")", Poco::Data::into(count);
  statement << " FROM " << name() << " WHERE " << kInsertTimeField << " <= ? ",
      Poco::Data::use(serialized_time);
  if (key != "") {
    statement << " AND " << key << " LIKE ";
    data_holder.push_back(value_holder.insertPlaceHolder(key, statement));
  }
  try {
    statement.execute();
  }
  catch (const std::exception& e) {
    LOG(FATAL) << "Count statement failed: " << statement.toString()
               << " with exception: " << e.what();
  }
  return count;
}

} /* namespace map_api */
