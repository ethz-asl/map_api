#include "map-api/cr-table.h"

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

const std::string CRTable::kIdField = "ID";
const std::string CRTable::kInsertTimeField = "insert_time";

std::pair<CRTable::RevisionMap::iterator, bool> CRTable::RevisionMap::insert(
    const std::shared_ptr<Revision>& revision) {
  CHECK_NOTNULL(revision.get());
  return insert(std::make_pair(revision->getId<Id>(), revision));
}

CRTable::~CRTable() {}

bool CRTable::init(std::unique_ptr<TableDescriptor>* descriptor) {
  CHECK_NOTNULL(descriptor);
  CHECK((*descriptor)->has_name());
  descriptor_ = std::move(*descriptor);
  CHECK(initCRDerived());
  initialized_ = true;
  return true;
}

bool CRTable::isInitialized() const { return initialized_; }

const std::string& CRTable::name() const {
  return descriptor_->name();
}

std::shared_ptr<Revision> CRTable::getTemplate() const {
  CHECK(isInitialized()) << "Can't get template of non-initialized table";
  std::shared_ptr<proto::Revision> proto(new proto::Revision);
  std::shared_ptr<Revision> ret =
      std::shared_ptr<Revision>(new Revision(proto));
  // add editable fields
  for (int i = 0; i < descriptor_->fields_size(); ++i) {
    ret->addField(i, descriptor_->fields(i));
  }
  return ret;
}

bool CRTable::insert(const LogicalTime& time, Revision* query) {
  CHECK_NOTNULL(query);
  CHECK(isInitialized()) << "Attempted to insert into non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(query->structureMatch(*reference))
      << "Bad structure of insert revision";
  CHECK(query->getId<Id>().isValid())
      << "Attempted to insert element with invalid ID";
  query->setInsertTime(time);
  return insertCRDerived(time, query);
}

bool CRTable::bulkInsert(const RevisionMap& query) {
  return bulkInsert(query, LogicalTime::sample());
}

bool CRTable::bulkInsert(const RevisionMap& query,
                         const LogicalTime& time) {
  CHECK(isInitialized()) << "Attempted to insert into non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  Id id;
  for (const RevisionMap::value_type& id_revision : query) {
    CHECK_NOTNULL(id_revision.second.get());
    CHECK(id_revision.second->structureMatch(*reference))
        << "Bad structure of insert revision";
    id = id_revision.second->getId<Id>();
    CHECK(id.isValid()) << "Attempted to insert element with invalid ID";
    CHECK(id == id_revision.first) << "ID in RevisionMap doesn't match";
    id_revision.second->setInsertTime(time);
  }
  return bulkInsertCRDerived(query, time);
}

bool CRTable::patch(const Revision& query) {
  CHECK(isInitialized()) << "Attempted to insert into non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(query.structureMatch(*reference)) << "Bad structure of patch revision";
  CHECK(query.getId<Id>().isValid())
      << "Attempted to insert element with invalid ID";
  return patchCRDerived(query);
}

void CRTable::dumpChunk(const Id& chunk_id, const LogicalTime& time,
                        RevisionMap* dest) {
  CHECK(isInitialized());
  CHECK_NOTNULL(dest);
  dest->clear();
  CHECK_LE(time, LogicalTime::sample());
  return dumpChunkCRDerived(chunk_id, time, dest);
}

int CRTable::findByRevision(int key, const Revision& valueHolder,
                            const LogicalTime& time, RevisionMap* dest) {
  CHECK(isInitialized()) << "Attempted to find in non-initialized table";
  // whether valueHolder contains key is implicitly checked whenever using
  // Revision::insertPlaceHolder - for now it's a pretty safe bet that the
  // implementation uses that - this would be rather cumbersome to check here
  CHECK_NOTNULL(dest);
  dest->clear();
  CHECK(time < LogicalTime::sample()) <<
      "Seeing the future is yet to be implemented ;)";
  return findByRevisionCRDerived(key, valueHolder, time, dest);
}

int CRTable::countByRevision(int key, const Revision& valueHolder,
                             const LogicalTime& time) {
  CHECK(isInitialized()) << "Attempted to count items in non-initialized table";
  // Whether valueHolder contains key is implicitly checked whenever using
  // Revision::insertPlaceHolder - for now it's a pretty safe bet that the
  // implementation uses that - this would be rather cumbersome to check here.
  CHECK(time < LogicalTime::sample())
      << "Seeing the future is yet to be implemented ;)";
  return countByRevisionCRDerived(key, valueHolder, time);
}

// although this is very similar to rawGetRow(), I don't see how to share the
// features without loss of performance TODO(discuss)
void CRTable::dump(const LogicalTime& time, RevisionMap* dest) {
  CHECK_NOTNULL(dest);
  std::shared_ptr<Revision> valueHolder = getTemplate();
  CHECK(valueHolder != nullptr);
  findByRevision(-1, *valueHolder, time, dest);
}

int CRTable::countByChunk(const Id& id, const LogicalTime& time) {
  CHECK(isInitialized());
  CHECK(time < LogicalTime::sample());
  return countByChunkCRDerived(id, time);
}

CRTable::Type CRTable::type() const {
  return Type::CR;
}

std::ostream& operator<<(std::ostream& stream,
                         const CRTable::ItemDebugInfo& info) {
  return stream << "For table " << info.table << ", item " << info.id << ": ";
}

} /* namespace map_api */
