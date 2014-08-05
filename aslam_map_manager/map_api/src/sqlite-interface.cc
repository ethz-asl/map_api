#include "map-api/sqlite-interface.h"

#include "map-api/id.h"

namespace map_api {

SqliteInterface::~SqliteInterface() {}

void SqliteInterface::init(std::weak_ptr<Poco::Data::Session> session) {
  session_ = session;
}

bool SqliteInterface::isSqlSafe(const TableDescriptor& descriptor) const {
  if (!isSqlSafe(descriptor.name())) {
    return false;
  }
  for (int i = 0; i < descriptor.fields_size(); ++i) {
    if (!isSqlSafe(descriptor.fields(i).name())) {
      return false;
    }
  }
  return true;
}

bool SqliteInterface::create(const TableDescriptor& descriptor) {
  std::shared_ptr<Poco::Data::Session> session = session_.lock();
  CHECK(session) << "Couldn't lock session weak pointer";
  Poco::Data::Statement stat(*session);
  stat << "CREATE TABLE IF NOT EXISTS " << descriptor.name() << " (";
  // parse fields from descriptor as database fields
  for (int i = 0; i < descriptor.fields_size(); ++i){
    const proto::TableFieldDescriptor &fieldDescriptor = descriptor.fields(i);
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
      case (proto::TableFieldDescriptor_Type_UINT64): stat << "INTEGER"; break;
      default:
        LOG(FATAL) << "Field type not handled";
    }
  }
  stat << ");";

  VLOG(3) << stat.toString();

  try {
    stat.execute();
  } catch(const std::exception &e){
    LOG(FATAL) << "Create failed with exception " << e.what();
  }

  return true;
}

bool SqliteInterface::insert(const Revision& to_insert) {
  // Bag for blobs that need to stay in scope until statement is executed
  std::vector<std::shared_ptr<Poco::Data::BLOB> > placeholderBlobs;

  // assemble SQLite statement
  std::shared_ptr<Poco::Data::Session> session = session_.lock();
  CHECK(session) << "Couldn't lock session weak pointer!";
  Poco::Data::Statement statement(*session);
  // NB: sqlite placeholders work only for column values
  statement << "INSERT INTO " << to_insert.table() << " ";

  statement << "(";
  for (int i = 0; i < to_insert.fieldqueries_size(); ++i) {
    if (i > 0){
      statement << ", ";
    }
    statement << to_insert.fieldqueries(i).nametype().name();
  }
  statement << ") VALUES ( ";
  for (int i = 0; i < to_insert.fieldqueries_size(); ++i) {
    if (i > 0){
      statement << " , ";
    }
    placeholderBlobs.push_back(to_insert.insertPlaceHolder(i, statement));
  }
  statement << " ); ";

  try {
    statement.execute();
  } catch(const std::exception &e) {
    LOG(FATAL) << "Insert failed with exception \"" << e.what() << "\", " <<
        " statement was \"" << statement.toString() << "\" and query :" <<
        to_insert.DebugString();
    system("cp database.db /tmp");
  }

  return true;
}

bool SqliteInterface::bulkInsert(const CRTable::RevisionMap& to_insert) {
  std::shared_ptr<Poco::Data::Session> session = session_.lock();
  CHECK(session) << "Couldn't lock session weak pointer!";
  *session << "BEGIN TRANSACTION", Poco::Data::now;
  for(const CRTable::RevisionMap::value_type& id_revision : to_insert) {
    CHECK(insert(*id_revision.second));
  }
  *session << "COMMIT", Poco::Data::now;
  return true;
}

std::weak_ptr<Poco::Data::Session> SqliteInterface::getSession() {
  return session_;
}

bool SqliteInterface::isSqlSafe(const std::string& string) const {
  for (const char& character : string) {
    if ((character < 'A' || character > 'Z') &&
        (character < 'a' || character > 'z') &&
        (character != '_')) {
      return false;
    }
  }
  return true;
}

SqliteInterface::PocoToProto::PocoToProto(
    const std::shared_ptr<Revision>& reference)
: reference_(reference) {}

void SqliteInterface::PocoToProto::into(Poco::Data::Statement& statement) {
  statement << " ";
  for (int i = 0; i < reference_->fieldqueries_size(); ++i) {
    if (i > 0) {
      statement << ", ";
    }
    const proto::TableField& field = reference_->fieldqueries(i);
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
      case (proto::TableFieldDescriptor_Type_UINT64):{
        statement, Poco::Data::into(ulongs_[field.nametype().name()]);
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

int SqliteInterface::PocoToProto::resultSize() const {
  if (doubles_.size()) return doubles_.begin()->second.size();
  if (ints_.size()) return ints_.begin()->second.size();
  if (longs_.size()) return longs_.begin()->second.size();
  if (ulongs_.size()) return ulongs_.begin()->second.size();
  if (blobs_.size()) return blobs_.begin()->second.size();
  if (strings_.size()) return strings_.begin()->second.size();
  if (hashes_.size()) return hashes_.begin()->second.size();
  return 0;
}

int SqliteInterface::PocoToProto::toProto(
    std::vector<std::shared_ptr<Revision> >* dest) const {
  CHECK_NOTNULL(dest);
  int size = resultSize();
  dest->resize(size);

  // write values
  for (size_t i = 0; i < dest->size(); ++i) {
    (*dest)[i] = std::make_shared<Revision>(*reference_);
  }

  // first by type, then by destination is necessary, otherwise cache-miss
  // heavy
  for (const std::pair<std::string, std::vector<double> >& fieldDouble :
      doubles_){
    for (size_t i = 0; i < dest->size(); ++i) {
      (*dest)[i]->set(fieldDouble.first, fieldDouble.second[i]);
    }
  }
  for (const std::pair<std::string, std::vector<int32_t> >& fieldInt :
      ints_){
    for (size_t i = 0; i < dest->size(); ++i) {
          (*dest)[i]->set(fieldInt.first, fieldInt.second[i]);
    }
  }
  for (const std::pair<std::string, std::vector<int64_t> >& fieldLong :
      longs_){
    for (size_t i = 0; i < dest->size(); ++i) {
          (*dest)[i]->set(fieldLong.first, fieldLong.second[i]);
    }
  }
  for (const std::pair<std::string, std::vector<uint64_t> >& fieldULong :
      ulongs_){
    for (size_t i = 0; i < dest->size(); ++i) {
          (*dest)[i]->set(fieldULong.first, fieldULong.second[i]);
    }
  }
  for (const std::pair<std::string, std::vector<Poco::Data::BLOB> >&
      fieldBlob : blobs_){
    for (size_t i = 0; i < dest->size(); ++i) {
          (*dest)[i]->set(fieldBlob.first, fieldBlob.second[i]);
    }
  }
  for (const std::pair<std::string, std::vector<std::string> >& fieldString :
      strings_){
    for (size_t i = 0; i < dest->size(); ++i) {
          (*dest)[i]->set(fieldString.first, fieldString.second[i]);
    }
  }
  for (const std::pair<std::string, std::vector<std::string> >& fieldHash :
      hashes_){
    for (size_t i = 0; i < dest->size(); ++i) {
          Id value;
    CHECK(value.fromHexString(fieldHash.second[i])) << "Can't parse id from "
        << fieldHash.second[i];
    (*dest)[i]->set(fieldHash.first, value);
    }
  }

  return size;
}

} /* namespace map_api */
