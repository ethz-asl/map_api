#include <map-api/cru-table-interface.h>

#include <cstdio>
#include <map>

#include <Poco/Data/Common.h>
#include <Poco/Data/Statement.h>
#include <Poco/Data/SQLite/Connector.h>
#include <Poco/Data/BLOB.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "map-api/map-api-core.h"
#include "map-api/transaction.h"
#include "core.pb.h"

namespace map_api {

CRTableInterface::~CRTableInterface() {}

bool CRTableInterface::isInitialized() const{
  return initialized_;
}

void CRTableInterface::addField(const std::string& name,
                                proto::TableFieldDescriptor_Type type){
  // make sure the field has not been defined yet
  for (int i=0; i<structure_.fields_size(); ++i){
    if (structure_.fields(i).name().compare(name) == 0){
      LOG(FATAL) << "In table " << structure_.name() << ": Field " << name <<
          " defined twice!" << std::endl;
    }
  }
  // otherwise, proceed with adding field
  proto::TableFieldDescriptor *field = structure_.add_fields();
  field->set_name(name);
  field->set_type(type);
}

bool CRTableInterface::init() {
  const std::string tableName(name());
  // verify name is SQL friendly: For now very tight constraints:
  for (const char& character : tableName) {
    CHECK((character >= 'A' && character <= 'Z') ||
          (character >= 'a' && character <= 'z') ||
          (character == '_')) << "Desired table name \"" << tableName <<
              "\" ill-suited for SQL database";
  }
  structure_.set_name(tableName);
  // Define table fields
  // enforced fields id (hash) and owner
  addField<Id>("ID");
  // addField<Id>("owner"); TODO(tcies) later, when owner will be used for
  // synchronization accross the network, or for its POC
  // transaction-enforced fields TODO(tcies) later
  // std::shared_ptr<std::vector<proto::TableFieldDescriptor> >
  // transactionFields(Transaction::requiredTableFields());
  // for (const proto::TableFieldDescriptor& descriptor :
  //     *transactionFields){
  //   addField(descriptor.name(), descriptor.type());
  // }
  // user-defined fields
  define();

  // connect to database & create table
  session_ = MapApiCore::getInstance().getSession();
  createQuery();

  // Sync with cluster TODO(tcies)
  if (!sync()){
    return false;
  }
  initialized_ = true;
  return true;
}

std::shared_ptr<Revision> CRTableInterface::getTemplate() const{
  CHECK(isInitialized()) << "Can't get template of non-initialized table";
  std::shared_ptr<Revision> ret =
      std::shared_ptr<Revision>(
          new Revision);
  // add own name
  ret->set_table(structure_.name());
  // add editable fields
  for (int i = 0; i < structure_.fields_size(); ++i){
    ret->addField(structure_.fields(i));
  }
  return ret;
}

bool CRTableInterface::sync() {
  return MapApiCore::getInstance().syncTableDefinition(structure_);
}

bool CRTableInterface::createQuery(){
  Poco::Data::Statement stat(*session_);
  stat << "CREATE TABLE IF NOT EXISTS " << name() << " (";
  // parse fields from descriptor as database fields
  for (int i = 0; i < structure_.fields_size(); ++i){
    const proto::TableFieldDescriptor &fieldDescriptor = structure_.fields(i);
    proto::TableField field;
    // The following is specified in protobuf but not available.
    // We are using an outdated version of protobuf.
    // Consider upgrading once overwhelmingly necessary.
    // field.set_allocated_nametype(&fieldDescriptor);
    *field.mutable_nametype() = fieldDescriptor;
    if (i != 0){
      stat << ", ";
    }
    stat << fieldDescriptor.name() << " ";
    switch (fieldDescriptor.type()){
      case (proto::TableFieldDescriptor_Type_BLOB): stat << "BLOB"; break;
      case (proto::TableFieldDescriptor_Type_DOUBLE): stat << "REAL"; break;
      case (proto::TableFieldDescriptor_Type_HASH128): stat << "TEXT"; break;
      case (proto::TableFieldDescriptor_Type_INT32): stat << "INTEGER"; break;
      case (proto::TableFieldDescriptor_Type_INT64): stat << "INTEGER"; break;
      case (proto::TableFieldDescriptor_Type_STRING): stat << "TEXT"; break;
      default:
        LOG(FATAL) << "Field type not handled";
    }
    if (fieldDescriptor.name().compare("ID") == 0){
      stat << " PRIMARY KEY";
    }
  }
  stat << ");";

  try {
    stat.execute();
  } catch(const std::exception &e){
    LOG(FATAL) << "Create failed with exception " << e.what();
  }

  return true;
}

bool CRTableInterface::rawInsert(Revision& query) const {
  CHECK(isInitialized()) << "Attempted to insert into non-initialized table";
  std::shared_ptr<Revision> reference = getTemplate();
  CHECK(reference->structureMatch(query)) << "Bad structure of insert revision";
  Id id;
  query.get("ID", &id);
  CHECK(id.isValid()) << "Attempted to insert element with invalid ID";
  return rawInsertImpl(query);
}
bool CRTableInterface::rawInsertImpl(Revision& query) const{
  // Bag for blobs that need to stay in scope until statement is executed
  std::vector<std::shared_ptr<Poco::Data::BLOB> > placeholderBlobs;

  // assemble SQLite statement
  Poco::Data::Statement stat(*session_);
  // NB: sqlite placeholders work only for column values
  stat << "INSERT INTO " << name() << " ";

  stat << "(";
  for (int i = 0; i < query.fieldqueries_size(); ++i){
    if (i > 0){
      stat << ", ";
    }
    stat << query.fieldqueries(i).nametype().name();
  }
  stat << ") VALUES ( ";
  for (int i = 0; i < query.fieldqueries_size(); ++i){
    if (i > 0){
      stat << " , ";
    }
    placeholderBlobs.push_back(query.insertPlaceHolder(i,stat));
  }
  stat << " ); ";

  try {
    stat.execute();
  } catch(const std::exception &e){
    LOG(FATAL) << "Insert failed with exception " << e.what();
  }

  return true;
}

std::shared_ptr<Revision> CRTableInterface::rawGetById(
    const Id &id) const{
  CHECK(isInitialized()) << "Attempted to insert into non-initialized table";
  CHECK_NE(id, Id()) << "Supplied invalid ID";
  return rawGetByIdImpl(id);
}
std::shared_ptr<Revision> CRTableInterface::rawGetByIdImpl(
    const Id &id) const{
  return rawFindUnique("ID", id);
}

int CRTableInterface::rawFindByRevision(
    const std::string& key, const Revision& valueHolder,
    std::vector<std::shared_ptr<Revision> >* dest) const {
  CHECK(isInitialized()) << "Attempted to find in non-initialized table";
  // whether valueHolder contains key is implicitly checked whenever using
  // Revision::insertPlaceHolder - for now it's a pretty safe bet that the
  // implementation uses that - this would be rather cumbersome to check here
  CHECK_NOTNULL(dest);
  return rawFindByRevisionImpl(key, valueHolder, dest);
}
// TODO(tcies) test
int CRTableInterface::rawFindByRevisionImpl(
    const std::string& key, const Revision& valueHolder,
    std::vector<std::shared_ptr<Revision> >* dest) const {
  PocoToProto pocoToProto(*this);
  Poco::Data::Statement statement(*session_);
  statement << "SELECT";
  pocoToProto.into(statement);
  statement << "FROM " << name();
  if (key != ""){
    statement << " WHERE " << key << " LIKE ";
    valueHolder.insertPlaceHolder(key, statement);
  }
  try{
    statement.execute();
  } catch (const std::exception& e){
    LOG(FATAL) << "Find statement failed: " << statement.toString() <<
        " with exception " << e.what();
  }
  return pocoToProto.toProto(dest);
}

// although this is very similar to rawGetRow(), I don't see how to share the
// features without loss of performance TODO(discuss)
void CRTableInterface::rawDump(std::vector<std::shared_ptr<Revision> >* dest)
const{
  std::shared_ptr<Revision> valueHolder = getTemplate();
  rawFindByRevision("", *valueHolder , dest);
}

CRTableInterface::PocoToProto::PocoToProto(
    const CRTableInterface& table) :
                table_(table) {}

void CRTableInterface::PocoToProto::into(Poco::Data::Statement& statement) {
  statement << " ";
  std::shared_ptr<Revision> dummy = table_.getTemplate();
  for (int i = 0; i < dummy->fieldqueries_size(); ++i) {
    if (i > 0) {
      statement << ", ";
    }
    const proto::TableField& field = dummy->fieldqueries(i);
    statement << field.nametype().name();
    switch(field.nametype().type()){
      case (proto::TableFieldDescriptor_Type_BLOB):{
        statement, Poco::Data::into(blobs_[field.nametype().name()]);
        break;
      }
      case (proto::TableFieldDescriptor_Type_DOUBLE):{
        statement, Poco::Data::into(doubles_[field.nametype().name()]);
        break;
      }
      case (proto::TableFieldDescriptor_Type_INT32):{
        statement, Poco::Data::into(ints_[field.nametype().name()]);
        break;
      }
      case (proto::TableFieldDescriptor_Type_INT64):{
        statement, Poco::Data::into(longs_[field.nametype().name()]);
        break;
      }
      case (proto::TableFieldDescriptor_Type_STRING):{
        statement, Poco::Data::into(strings_[field.nametype().name()]);
        break;
      }
      case (proto::TableFieldDescriptor_Type_HASH128):{
        statement, Poco::Data::into(hashes_[field.nametype().name()]);
        break;
      }
      default:{
        LOG(FATAL) << "Type of field supplied to select query unknown";
      }
    }
  }
  statement << " ";
}

int CRTableInterface::PocoToProto::toProto(
    std::vector<std::shared_ptr<Revision> >* dest) {
  CHECK_NOTNULL(dest);
  // reserve output size
  CHECK(hashes_.find("ID") != hashes_.end());
  dest->resize(hashes_["ID"].size());

  // write values
  for (size_t i = 0; i < dest->size(); ++i) {
    (*dest)[i] = table_.getTemplate();
    for (const std::pair<std::string, std::vector<double> >& fieldDouble :
        doubles_){
      (*dest)[i]->set(fieldDouble.first, fieldDouble.second[i]);
    }
    for (const std::pair<std::string, std::vector<int32_t> >& fieldInt :
        ints_){
      (*dest)[i]->set(fieldInt.first, fieldInt.second[i]);
    }
    for (const std::pair<std::string, std::vector<int64_t> >& fieldLong :
        longs_){
      (*dest)[i]->set(fieldLong.first, fieldLong.second[i]);
    }
    for (const std::pair<std::string, std::vector<Poco::Data::BLOB> >&
        fieldBlob : blobs_){
      (*dest)[i]->set(fieldBlob.first, fieldBlob.second[i]);
    }
    for (const std::pair<std::string, std::vector<std::string> >& fieldString :
        strings_){
      (*dest)[i]->set(fieldString.first, fieldString.second[i]);
    }
    for (const std::pair<std::string, std::vector<std::string> >& fieldHash :
        hashes_){
      Id value;
      CHECK(value.fromHexString(fieldHash.second[i])) << "Can't parse id from "
          << fieldHash.second[i];
      (*dest)[i]->set(fieldHash.first, value);
    }
  }

  return hashes_["ID"].size();
}

std::ostream& operator<< (std::ostream& stream,
                          const CRTableInterface::ItemDebugInfo& info){
  return stream << "For table " << info.table << ", item " << info.id << ": ";
}

} /* namespace map_api */
