#include "map-api/sqlite-interface.h"
#include "map-api/unique-id.h"

namespace map_api {

const std::string kCustomPrefix = "custom_";

SqliteInterface::~SqliteInterface() {}

void SqliteInterface::init(const std::string& table_name,
                           std::weak_ptr<Poco::Data::Session> session) {
  session_ = session;
  table_name_ = table_name;
}

bool SqliteInterface::isSqlSafe(const TableDescriptor& descriptor) const {
  if (!isSqlSafe(descriptor.name())) {
    return false;
  }
  return true;
}

bool __attribute__((deprecated))
    SqliteInterface::create(const TableDescriptor& descriptor) {
  LOG(FATAL) << "Not implemented";  // TODO(tcies) default fields
  std::shared_ptr<Poco::Data::Session> session = session_.lock();
  CHECK(session) << "Couldn't lock session weak pointer";
  Poco::Data::Statement stat(*session);
  stat << "CREATE TABLE IF NOT EXISTS " << descriptor.name() << " (";
  // parse fields from descriptor as database fields
  for (int i = 0; i < descriptor.fields_size(); ++i) {
    const proto::Type& type = descriptor.fields(i);
    proto::TableField field;
    // The following is specified in protobuf but not available.
    // We are using an outdated version of protobuf.
    // Consider upgrading once overwhelmingly necessary.
    // field.set_allocated_nametype(&fieldDescriptor);
    field.set_type(type);
    if (i != 0) {
      stat << ", ";
    }
    stat << kCustomPrefix << i << " ";
    switch (type) {
      case proto::Type::BLOB:
        stat << "BLOB";
        break;
      case proto::Type::DOUBLE:
        stat << "REAL";
        break;
      case proto::Type::HASH128:
        stat << "TEXT";
        break;
      case proto::Type::INT32:
        stat << "INTEGER";
        break;
      case proto::Type::INT64:
        stat << "INTEGER";
        break;
      case proto::Type::STRING:
        stat << "TEXT";
        break;
      case proto::Type::UINT64:
        stat << "INTEGER";
        break;
      default:
        LOG(FATAL) << "Field type not handled";
    }
  }
  stat << ");";

  VLOG(3) << stat.toString();

  try {
    stat.execute();
  }
  catch (const std::exception& e) {  // NOLINT
    LOG(FATAL) << "Create failed with exception " << e.what();
  }

  return true;
}

bool __attribute__((deprecated))
    SqliteInterface::insert(const Revision& to_insert) {
  // Bag for blobs that need to stay in scope until statement is executed
  std::vector<std::shared_ptr<Poco::Data::BLOB> > placeholderBlobs;

  // assemble SQLite statement
  std::shared_ptr<Poco::Data::Session> session = session_.lock();
  CHECK(session) << "Couldn't lock session weak pointer!";
  Poco::Data::Statement statement(*session);
  // NB: sqlite placeholders work only for column values
  statement << "INSERT INTO " << table_name_ << " ";

  statement << "(";
  LOG(FATAL) << "Not implemented";  // TODO(tcies) default fields
  int custom_field_count = to_insert.customFieldCount();
  for (int i = 0; i < custom_field_count; ++i) {
    if (i > 0) {
      statement << ", ";
    }
    statement << kCustomPrefix << i;
  }
  statement << ") VALUES ( ";
  for (int i = 0; i < custom_field_count; ++i) {
    if (i > 0) {
      statement << " , ";
    }
    placeholderBlobs.push_back(to_insert.insertPlaceHolder(i, &statement));
  }
  statement << " ); ";

  try {
    statement.execute();
  } catch(const std::exception &e) {
    LOG(FATAL) << "Insert failed with exception \"" << e.what() << "\", "
               << " statement was \"" << statement.toString()
               << "\" and query :" << to_insert.dumpToString();
    system("cp database.db /tmp");
  }

  return true;
}

bool SqliteInterface::bulkInsert(
    const CRTable::NonConstRevisionMap& to_insert) {
  std::shared_ptr<Poco::Data::Session> session = session_.lock();
  CHECK(session) << "Couldn't lock session weak pointer!";
  *session << "BEGIN TRANSACTION", Poco::Data::now;
  for (const CRTable::RevisionMap::value_type& id_revision : to_insert) {
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

void __attribute__((deprecated))
    SqliteInterface::PocoToProto::into(Poco::Data::Statement* statement) {
  CHECK_NOTNULL(statement);
  LOG(FATAL) << "Not implemented";  // TODO(tcies) default fields
  *statement << " ";
  for (int i = 0; i < reference_->customFieldCount(); ++i) {
    if (i > 0) {
      *statement << ", ";
    }
    proto::Type type = reference_->getFieldType(i);
    *statement << kCustomPrefix << i;
    switch (type) {
      case proto::Type::BLOB: {
        *statement, Poco::Data::into(blobs_[i]);
        break;
      }
      case proto::Type::DOUBLE: {
        *statement, Poco::Data::into(doubles_[i]);
        break;
      }
      case proto::Type::INT32: {
        *statement, Poco::Data::into(ints_[i]);
        break;
      }
      case proto::Type::INT64: {
        *statement, Poco::Data::into(longs_[i]);
        break;
      }
      case proto::Type::UINT64: {
        *statement, Poco::Data::into(ulongs_[i]);
        break;
      }
      case proto::Type::STRING: {
        *statement, Poco::Data::into(strings_[i]);
        break;
      }
      case proto::Type::HASH128: {
        *statement, Poco::Data::into(hashes_[i]);
        break;
      }
      default: {
        LOG(FATAL) << "Type of field supplied to select query unknown";
      }
    }
  }
  *statement << " ";
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

int __attribute__((deprecated)) SqliteInterface::PocoToProto::toProto(
    std::vector<std::shared_ptr<Revision> >* dest) const {
  CHECK_NOTNULL(dest);
  LOG(FATAL) << "Not implemented";  // TODO(tcies) implement
  int size = resultSize();
  dest->resize(size);

  // write values
  for (size_t i = 0; i < dest->size(); ++i) {
    (*dest)[i] = std::make_shared<Revision>(*reference_);
  }

  // first by type, then by destination is necessary, otherwise cache-miss
  // heavy
  for (const std::pair<int, std::vector<double> >& fieldDouble : doubles_) {
    for (size_t i = 0; i < dest->size(); ++i) {
      (*dest)[i]->set(fieldDouble.first, fieldDouble.second[i]);
    }
  }
  for (const std::pair<int, std::vector<Poco::Int32> >& fieldInt : ints_) {
    for (size_t i = 0; i < dest->size(); ++i) {
      (*dest)[i]->set(fieldInt.first, static_cast<int32_t>(fieldInt.second[i]));
    }
  }
  for (const std::pair<int, std::vector<Poco::Int64> >& fieldLong : longs_) {
    for (size_t i = 0; i < dest->size(); ++i) {
      (*dest)[i]
          ->set(fieldLong.first, static_cast<int64_t>(fieldLong.second[i]));
    }
  }
  for (const std::pair<int, std::vector<Poco::UInt64> >& fieldULong : ulongs_) {
    for (size_t i = 0; i < dest->size(); ++i) {
      (*dest)[i]
          ->set(fieldULong.first, static_cast<uint64_t>(fieldULong.second[i]));
    }
  }
  for (const std::pair<int, std::vector<Poco::Data::BLOB> >& fieldBlob :
       blobs_) {
    for (size_t i = 0; i < dest->size(); ++i) {
      (*dest)[i]->set(fieldBlob.first, fieldBlob.second[i]);
    }
  }
  for (const std::pair<int, std::vector<std::string> >& fieldString :
       strings_) {
    for (size_t i = 0; i < dest->size(); ++i) {
      (*dest)[i]->set(fieldString.first, fieldString.second[i]);
    }
  }
  for (const std::pair<int, std::vector<std::string> >& fieldHash : hashes_) {
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
