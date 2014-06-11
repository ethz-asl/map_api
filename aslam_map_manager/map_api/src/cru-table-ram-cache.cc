#include "cru-table-ram-cache.h"

namespace map_api {

CRUTableRAMCache::~CRUTableRAMCache() {}

bool CRUTableRAMCache::initCRUDerived() {
  sqlite_interface_.init(MapApiCore::instance().getSession());
  CHECK(sqlite_interface_.isSqlSafe(*descriptor_));
  CHECK(sqlite_interface_.create(*descriptor_));
  return true;
}

bool CRUTableRAMCache::insertCRUDerived(Revision* query) {
  return sqlite_interface_.insert(*query);
}

int CRUTableRAMCache::findByRevisionCRUDerived(
    const std::string& key, const Revision& valueHolder, const Time& time,
    std::unordered_map<Id, std::shared_ptr<Revision> >* dest) {
  // TODO(tcies) apart from the more sophisticated time query, this is very
  // similar to its CR equivalent. Maybe refactor at some time?
  SqliteInterface::PocoToProto poco_to_proto(*this);
  std::shared_ptr<Poco::Data::Session> session =
      sqlite_interface_.getSession().lock();
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

bool CRUTableRAMCache::insertUpdatedCRUDerived(const Revision& query) {
  sqlite_interface_.insert(query);
  return true;
}

bool CRUTableRAMCache::updateCurrentReferToUpdatedCRUDerived(
    const Id& id, const Time& current_time, const Time& updated_time) {
  std::shared_ptr<Poco::Data::Session> session =
      sqlite_interface_.getSession().lock();
  CHECK(session) << "Couldn't lock session weak pointer";
  Poco::Data::Statement statement(*session);
  statement << "UPDATE " << name() << " SET " << kNextTimeField << " = ? ",
      Poco::Data::use(updated_time.serialize());
  statement << " WHERE ID = ? ",
      Poco::Data::use(id.hexString());
  statement << " AND " << kUpdateTimeField << " = ? ",
      Poco::Data::use(current_time.serialize());
  try {
    statement.execute();
  } catch (const std::exception& e) {
    LOG(FATAL) << info << kNextTimeField << " update failed with exception \""
        << e.what() << "\", " << " statement was \"" << statement.toString() <<
        "\" with Id and times :" << id << " " << current_time << " " <<
        updated_time;
  }
  return true;
}

} /* namespace map_api */
