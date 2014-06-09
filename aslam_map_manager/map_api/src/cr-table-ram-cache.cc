#include "map-api/cr-table-ram-cache.h"

#include <glog/logging.h>

namespace map_api {

CRTableRAMCache::~CRTableRAMCache() {}

bool CRTableRAMCache::initCRDerived() {
  CHECK(isSqlSafe(descriptor_->name()));
  for (int i = 0; i < descriptor_->fields_size(); ++i) {
    CHECK(isSqlSafe(descriptor_->fields(i).name()));
  }
  // connect to database & create table
  session_ = MapApiCore::instance().getSession();
  createQuery();
  return true;
}

void CRTableRAMCache::defineDefaultFieldsCRDerived() {}
void CRTableRAMCache::ensureDefaulFieldsCRDerived(Revision* query) const {}

bool CRTableRAMCache::insertCRDerived(Revision* query) {
  // Bag for blobs that need to stay in scope until statement is executed
  std::vector<std::shared_ptr<Poco::Data::BLOB> > placeholderBlobs;

  // assemble SQLite statement
  std::shared_ptr<Poco::Data::Session> session = session_.lock();
  CHECK(session) << "Couldn't lock session weak pointer!";
  Poco::Data::Statement statement(*session);
  // NB: sqlite placeholders work only for column values
  statement << "INSERT INTO " << name() << " ";

  statement << "(";
  for (int i = 0; i < query->fieldqueries_size(); ++i) {
    if (i > 0){
      statement << ", ";
    }
    statement << query->fieldqueries(i).nametype().name();
  }
  statement << ") VALUES ( ";
  for (int i = 0; i < query->fieldqueries_size(); ++i) {
    if (i > 0){
      statement << " , ";
    }
    placeholderBlobs.push_back(query->insertPlaceHolder(i, statement));
  }
  statement << " ); ";

  try {
    statement.execute();
  } catch(const std::exception &e) {
    LOG(FATAL) << "Insert failed with exception \"" << e.what() << "\", " <<
        " statement was \"" << statement.toString() << "\" and query :" <<
        query->DebugString();
  }

  return true;
}

int CRTableRAMCache::findByRevisionCRDerived(
    const std::string& key, const Revision& valueHolder, const Time& time,
    std::unordered_map<Id, std::shared_ptr<Revision> >* dest) {
  PocoToProto pocoToProto(*this);
  std::shared_ptr<Poco::Data::Session> session = session_.lock();
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

bool CRTableRAMCache::isSqlSafe(const std::string& string) const {
  for (const char& character : string) {
    if ((character < 'A' || character > 'Z') &&
        (character < 'a' || character > 'z') &&
        (character != '_')) {
      return false;
    }
  }
  return true;
}

bool CRTableRAMCache::createQuery(){
  std::shared_ptr<Poco::Data::Session> session = session_.lock();
  CHECK(session) << "Couldn't lock session weak pointer";
  Poco::Data::Statement stat(*session);
  stat << "CREATE TABLE IF NOT EXISTS " << name() << " (";
  // parse fields from descriptor as database fields
  for (int i = 0; i < descriptor_->fields_size(); ++i){
    const proto::TableFieldDescriptor &fieldDescriptor = descriptor_->fields(i);
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
  }
  stat << ");";

  try {
    stat.execute();
  } catch(const std::exception &e){
    LOG(FATAL) << "Create failed with exception " << e.what();
  }

  return true;
}

CRTableRAMCache::PocoToProto::PocoToProto(const CRTable& table) :
                                                table_(table) {}

void CRTableRAMCache::PocoToProto::into(Poco::Data::Statement& statement) {
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

int CRTableRAMCache::PocoToProto::toProto(
    std::vector<std::shared_ptr<Revision> >* dest) {
  CHECK_NOTNULL(dest);
  // reserve output size
  const std::map<std::string, std::vector<std::string> >::iterator
  id_hashes_iterator = hashes_.find(kIdField);
  CHECK(id_hashes_iterator != hashes_.end());
  std::vector<std::string>& id_hashes = id_hashes_iterator->second;
  dest->resize(id_hashes.size());

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

  return id_hashes.size();
}

} /* namespace map_api */
