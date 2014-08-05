#include "map-api/cr-table.h"

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

const std::string CRTable::kIdField = "ID";
const std::string CRTable::kInsertTimeField = "insert_time";

CRTable::~CRTable() {}

bool CRTable::init(std::unique_ptr<TableDescriptor>* descriptor) {
  CHECK_NOTNULL(descriptor);
  CHECK((*descriptor)->has_name());
  descriptor_ = std::move(*descriptor);
  descriptor_->addField<Id>(kIdField);
  descriptor_->addField<LogicalTime>(kInsertTimeField);
  CHECK(initCRDerived());
  initialized_ = true;
  return true;
}

bool CRTable::isInitialized() const{
  return initialized_;
}

const std::string& CRTable::name() const {
  return descriptor_->name();
}

std::shared_ptr<Revision> CRTable::getTemplate() const{
  CHECK(isInitialized()) << "Can't get template of non-initialized table";
  std::shared_ptr<Revision> ret =
      std::shared_ptr<Revision>(
          new Revision);
  // add own name
  ret->set_table(descriptor_->name());
  // add editable fields
  for (int i = 0; i < descriptor_->fields_size(); ++i){
    ret->addField(descriptor_->fields(i));
  }
  return ret;
}

bool CRTable::insert(Revision* query) {
  CHECK_NOTNULL(query);
  CHECK(isInitialized()) << "Attempted to insert into non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(reference->structureMatch(*query)) <<
      "Bad structure of insert revision";
  Id id;
  query->get(kIdField, &id);
  CHECK(id.isValid()) << "Attempted to insert element with invalid ID";
  query->set(kInsertTimeField, LogicalTime::sample());
  return insertCRDerived(query);
}

bool CRTable::bulkInsert(const RevisionMap& query) {
  CHECK(isInitialized()) << "Attempted to insert into non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  Id id;
  for (const RevisionMap::value_type& id_revision : query) {
    CHECK_NOTNULL(id_revision.second.get());
    CHECK(reference->structureMatch(*id_revision.second)) <<
        "Bad structure of insert revision";
    id_revision.second->get(kIdField, &id);
    CHECK(id.isValid()) << "Attempted to insert element with invalid ID";
    CHECK(id == id_revision.first) << "ID in RevisionMap doesn't match";
    id_revision.second->set(kInsertTimeField, LogicalTime::sample());
  }
  return bulkInsertCRDerived(query);
}

bool CRTable::patch(const Revision& query) {
  CHECK(isInitialized()) << "Attempted to insert into non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(reference->structureMatch(query)) << "Bad structure of patch revision";
  Id id;
  query.get(kIdField, &id);
  CHECK(id.isValid()) << "Attempted to insert element with invalid ID";
  return patchCRDerived(query);
}

std::shared_ptr<Revision> CRTable::getById(
    const Id &id, const LogicalTime& time) {
  CHECK(isInitialized()) << "Attempted to getById from non-initialized table";
  CHECK_NE(id, Id()) << "Supplied invalid ID";
  return findUnique(kIdField, id, time);
}

int CRTable::findByRevision(
    const std::string& key, const Revision& valueHolder,
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

// although this is very similar to rawGetRow(), I don't see how to share the
// features without loss of performance TODO(discuss)
void CRTable::dump(const LogicalTime& time, RevisionMap* dest)
{
  std::shared_ptr<Revision> valueHolder = getTemplate();
  findByRevision("", *valueHolder, time, dest);
}

CRTable::Type CRTable::type() const {
  return Type::CR;
}

std::ostream& operator<< (std::ostream& stream,
                          const CRTable::ItemDebugInfo& info){
  return stream << "For table " << info.table << ", item " << info.id << ": ";
}

} /* namespace map_api */
