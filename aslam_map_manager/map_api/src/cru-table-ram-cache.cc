#include "map-api/cru-table-ram-cache.h"

#include "map-api/map-api-core.h"

namespace map_api {

CRUTableRAMCache::~CRUTableRAMCache() {}

bool CRUTableRAMCache::initCRUDerived() {
  sqlite_interface_.init(MapApiCore::getSession());
  CHECK(sqlite_interface_.isSqlSafe(*descriptor_));
  CHECK(sqlite_interface_.create(*descriptor_));
  return true;
}

bool CRUTableRAMCache::insertCRUDerived(Revision* query) {
  return sqlite_interface_.insert(*query);
}

bool CRUTableRAMCache::bulkInsertCRUDerived(const RevisionMap& query) {
  return sqlite_interface_.bulkInsert(query);
}

bool CRUTableRAMCache::patchCRDerived(const Revision& query) {
  return sqlite_interface_.insert(query);
}

int CRUTableRAMCache::findByRevisionCRUDerived(
    const std::string& key, const Revision& value_holder,
    const LogicalTime& time, RevisionMap* dest) {
  // TODO(tcies) apart from the more sophisticated time query, this is very
  // similar to its CR equivalent. Maybe refactor at some time?
  SqliteInterface::PocoToProto poco_to_proto(getTemplate());
  std::shared_ptr<Poco::Data::Session> session =
      sqlite_interface_.getSession().lock();
  CHECK(session) << "Couldn't lock session weak pointer";
  Poco::Data::Statement statement(*session);
  // caching of data needed for Poco::Data to work
  uint64_t serialized_time = time.serialize();
  std::vector<std::shared_ptr<Poco::Data::BLOB> > data_holder;
  // TODO(tcies) evt. optimizations from http://www.sqlite.org/queryplanner.html
  statement << "SELECT ";
  poco_to_proto.into(statement);
  statement << " FROM " << name() << " WHERE " << kUpdateTimeField << " <  ? ",
      Poco::Data::use(serialized_time);
  if (FLAGS_cru_linked) {
    statement << " AND (" << kNextTimeField << " = 0 OR " << kNextTimeField <<
        " > ? ", Poco::Data::use(serialized_time);
    statement << ") ";
  }
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
  poco_to_proto.toProto(&from_poco);
  for (const std::shared_ptr<Revision>& item : from_poco) {
    Id id;
    item->get(kIdField, &id);
    CHECK(id.isValid());
    std::unordered_map<Id, LogicalTime> latest;
    if (FLAGS_cru_linked) {
      // case linked: query guarantees that each ID is unique
      if (!dest->insert(std::make_pair(id, item)).second) {
        std::ostringstream report;
        report << "Failed to insert:" << std::endl;
        report << item->dumpToString() << std::endl;
        report << "Into map with:";
        for (const std::pair<Id, std::shared_ptr<Revision> >& in_dest : *dest) {
          report << in_dest.second->dumpToString() << std::endl;
        }
        LOG(FATAL) << report.str();
      }
    } else {
      // case not linked: result contains entire history of each ID up to the
      // specified time, need to return latest only
      LogicalTime item_time;
      item->get(kUpdateTimeField, &item_time);
      if (item_time > latest[id]) {
        (*dest)[id] = item;
        latest[id] = item_time;
      }
    }
  }
  return dest->size();
}

int CRUTableRAMCache::countByRevisionCRUDerived(const std::string& key,
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
  uint64_t serialized_time = time.serialize();
  std::vector<std::shared_ptr<Poco::Data::BLOB> > data_holder;
  int count = 0;
  statement << "SELECT COUNT(" << kIdField << ")", Poco::Data::into(count);
  statement << " FROM " << name() << " WHERE " << kUpdateTimeField << " <  ? ",
      Poco::Data::use(serialized_time);
  if (FLAGS_cru_linked) {
    statement << " AND (" << kNextTimeField << " = 0 OR " << kNextTimeField
              << " > ? ",
        Poco::Data::use(serialized_time);
    statement << ") ";
  }
  if (key != "") {
    statement << " AND " << key << " LIKE ";
    data_holder.push_back(value_holder.insertPlaceHolder(key, statement));
  }
  try {
    statement.execute();
  }
  catch (const std::exception& e) {
    LOG(FATAL) << "Find statement failed: " << statement.toString()
               << " with exception: " << e.what();
  }
  return count;
}

bool CRUTableRAMCache::insertUpdatedCRUDerived(const Revision& query) {
  sqlite_interface_.insert(query);
  return true;
}

bool CRUTableRAMCache::updateCurrentReferToUpdatedCRUDerived(
    const Id& id, const LogicalTime& current_time,
    const LogicalTime& updated_time) {
  CHECK(FLAGS_cru_linked);
  ItemDebugInfo info(this->name(), id);
  std::shared_ptr<Poco::Data::Session> session =
      sqlite_interface_.getSession().lock();
  CHECK(session) << "Couldn't lock session weak pointer";
  Poco::Data::Statement statement(*session);
  // caching of data needed for Poco::Data to work
  uint64_t serialized_update_time = updated_time.serialize(),
      serialized_current_time = current_time.serialize();
  std::string id_string = id.hexString();
  statement << "UPDATE " << name() << " SET " << kNextTimeField << " = ? ",
      Poco::Data::use(serialized_update_time);
  statement << " WHERE ID = ? ",
      Poco::Data::use(id_string);
  statement << " AND " << kUpdateTimeField << " = ? ",
      Poco::Data::use(serialized_current_time);
  try {
    statement.execute();
  }
  catch (const std::exception& e) {
    LOG(FATAL) << info << kNextTimeField << " update failed with exception \""
        << e.what() << "\", " << " statement was \"" << statement.toString() <<
        "\" with times: " << current_time << " " << updated_time;
  }
  return true;
}

} /* namespace map_api */
