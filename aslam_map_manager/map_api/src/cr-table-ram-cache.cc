#include "map-api/cr-table-ram-cache.h"

#include <glog/logging.h>

#include "map-api/map-api-core.h"

namespace map_api {

CRTableRAMCache::~CRTableRAMCache() {}

bool CRTableRAMCache::initCRDerived() {
  sqlite_interface_.init(MapApiCore::instance().getSession());
  CHECK(sqlite_interface_.isSqlSafe(*descriptor_));
  CHECK(sqlite_interface_.create(*descriptor_));
  return true;
}

bool CRTableRAMCache::insertCRDerived(Revision* query) {
  return sqlite_interface_.insert(*query);
}

int CRTableRAMCache::findByRevisionCRDerived(
    const std::string& key, const Revision& valueHolder, const Time& time,
    std::unordered_map<Id, std::shared_ptr<Revision> >* dest) {
  SqliteInterface::PocoToProto pocoToProto(getTemplate());
  std::shared_ptr<Poco::Data::Session> session =
      sqlite_interface_.getSession().lock();
  CHECK(session) << "Couldn't lock session weak pointer";
  Poco::Data::Statement statement(*session);
  statement << "SELECT";
  pocoToProto.into(statement);
  statement << "FROM " << name() << " WHERE " << kInsertTimeField << " <= ? ",
      Poco::Data::use(time.serialize());
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
  pocoToProto.toProto(&from_poco);
  for (const std::shared_ptr<Revision>& item : from_poco) {
    Id id;
    item->get(kIdField, &id);
    CHECK(id.isValid());
    (*dest)[id] = item;
  }
  return from_poco.size();
}

} /* namespace map_api */
