#include <glog/logging.h>
#include <Poco/Data/Common.h>
#include <Poco/Data/BLOB.h>

#include <map-api/logical-time.h>
#include <map-api/revision.h>
#include <map-api/unique-id.h>

namespace map_api {

Revision::Revision(const std::shared_ptr<proto::Revision>& revision)
    : underlying_revision_(revision) {}

std::shared_ptr<Poco::Data::BLOB> Revision::insertPlaceHolder(
    int index, Poco::Data::Statement* stat) const {
  CHECK_NOTNULL(stat);
  std::shared_ptr<Poco::Data::BLOB> blobPointer;
  const proto::TableField& field =
      underlying_revision_->custom_field_values(index);
  CHECK(field.has_type()) << "Trying to insert placeholder of undefined type";
  *stat << " ";
  switch (field.type()) {
    case proto::Type_BLOB: {
      blobPointer = std::make_shared<Poco::Data::BLOB>(
          Poco::Data::BLOB(field.blob_value()));
      *stat << "?", Poco::Data::use(*blobPointer);
      break;
    }
    case proto::Type_DOUBLE: {
      *stat << field.double_value();
      break;
    }
    case proto::Type_HASH128: {
      *stat << "?", Poco::Data::use(field.string_value());
      break;
    }
    case proto::Type_INT32: {
      *stat << field.int_value();
      break;
    }
    case(proto::Type_UINT32) : {
      *stat << field.uint_value();
      break;
    }
    case proto::Type_INT64: {
      *stat << field.long_value();
      break;
    }
    case proto::Type_UINT64: {
      *stat << field.ulong_value();
      break;
    }
    case proto::Type_STRING: {
      *stat << "?", Poco::Data::use(field.string_value());
      break;
    }
    default:
      LOG(FATAL) << "Field type not handled";
      return blobPointer;
  }
  *stat << " ";
  return blobPointer;
}

std::shared_ptr<Poco::Data::BLOB> Revision::insertPlaceHolder(
    const std::string& field, Poco::Data::Statement* stat) const {
  FieldMap::const_iterator fieldIt = fields_.find(field);
  CHECK(fieldIt != fields_.end()) << "Attempted to access inexisting field " <<
      field;
  return insertPlaceHolder(fieldIt->second, stat);
}

void Revision::addField(const proto::TableFieldDescriptor& descriptor) {
  // add field
  *add_fieldqueries()->mutable_nametype() = descriptor;
  // add to index
  fields_[descriptor.name()] = fieldqueries_size() - 1;
}

bool Revision::structureMatch(const Revision& other) const {
  if (fields_.size() != other.fields_.size()) {
    LOG(INFO) << "Field count does not match";
    return false;
  }
  FieldMap::const_iterator leftIterator = fields_.begin(),
      rightIterator = other.fields_.begin();
  while (leftIterator != fields_.end()) {
    if (leftIterator->first != rightIterator->first) {
      LOG(INFO) << "Field name mismatch: " << leftIterator->first << " vs " <<
          rightIterator->first;
      return false;
    }
    ++leftIterator;
    ++rightIterator;
  }
  return true;
}

bool Revision::fieldMatch(const Revision& other, int key) const {
  const proto::TableField& a = fieldqueries(key);
  const proto::TableField& b = other.fieldqueries(key);
  switch (a.type()) {
    case proto::Type::BLOB: { return a.blob_value() == b.blob_value(); }
    case(proto::Type::DOUBLE) : { return a.double_value() == b.double_value(); }
    case(proto::Type::HASH128) : {
      return a.string_value() == b.string_value();
    }
    case(proto::Type::INT32) : { return a.int_value() == b.int_value(); }
    case(proto::Type::UINT32) : { return a.uint_value() == b.uint_value(); }
    case(proto::Type::INT64) : { return a.long_value() == b.long_value(); }
    case(proto::Type::UINT64) : { return a.ulong_value() == b.ulong_value(); }
    case(proto::Type::STRING) : { return a.string_value() == b.string_value(); }
  }
  CHECK(false) << "Forgot switch case";
  return false;
}

int Revision::indexOf(const std::string& key) const {
  FieldMap::const_iterator index = fields_.find(key);
  // assuming same index for speedup
  CHECK(index != fields_.end()) << "Trying to get inexistent field";
  return index->second;
}

bool Revision::ParseFromString(const std::string& data) {
  bool success = proto::Revision::ParseFromString(data);
  CHECK(success) << "Parsing revision from string failed";
  for (int i = 0; i < fieldqueries_size(); ++i) {
    fields_[fieldqueries(i).nametype().name()] = i;
  }
  return true;
}

std::string Revision::dumpToString() const {
  std::ostringstream dump_ss;
  dump_ss << "Table " << table() << ": {" << std::endl;
  for (const std::pair<const std::string, int>& name_field : fields_) {
    dump_ss << "\t" << name_field.first << ": ";
    const proto::TableField& field = fieldqueries(name_field.second);
    if (field.has_blob_value()) dump_ss << field.blob_value();
    if (field.has_double_value()) dump_ss << field.double_value();
    if (field.has_int_value()) dump_ss << field.int_value();
    if (field.has_uint_value()) dump_ss << field.uint_value();
    if (field.has_long_value()) dump_ss << field.long_value();
    if (field.has_ulong_value()) dump_ss << field.ulong_value();
    if (field.has_string_value()) dump_ss << field.string_value();
    dump_ss << std::endl;
  }
  dump_ss << "}" << std::endl;
  return dump_ss.str();
}

/**
 * PROTOBUFENUM
 */

MAP_API_TYPE_ENUM(std::string, proto::Type::STRING);
MAP_API_TYPE_ENUM(double, proto::Type::DOUBLE);
MAP_API_TYPE_ENUM(int32_t, proto::Type::INT32);
MAP_API_TYPE_ENUM(uint32_t, proto::Type::UINT32);
MAP_API_TYPE_ENUM(bool, proto::Type::INT32);
MAP_API_TYPE_ENUM(Id, proto::Type::HASH128);
MAP_API_TYPE_ENUM(sm::HashId, proto::Type::HASH128);
MAP_API_TYPE_ENUM(int64_t, proto::Type::INT64);
MAP_API_TYPE_ENUM(uint64_t, proto::Type::UINT64);
MAP_API_TYPE_ENUM(LogicalTime, proto::Type::UINT64);
MAP_API_TYPE_ENUM(Revision, proto::Type::BLOB);
MAP_API_TYPE_ENUM(testBlob, proto::Type::BLOB);
MAP_API_TYPE_ENUM(Poco::Data::BLOB, proto::Type::BLOB);

/**
 * SET
 */
MAP_API_REVISION_SET(std::string) {
  field.set_stringvalue(value);
  return true;
}
MAP_API_REVISION_SET(double) {  // NOLINT ("All parameters should be named ...")
  field.set_doublevalue(value);
  return true;
}
MAP_API_REVISION_SET(int32_t) {
  field.set_intvalue(value);
  return true;
}
MAP_API_REVISION_SET(uint32_t) {
  field.set_uintvalue(value);
  return true;
}
MAP_API_REVISION_SET(bool) {  // NOLINT ("All parameters should be named ...")
  field.set_intvalue(value ? 1 : 0);
  return true;
}
MAP_API_REVISION_SET(Id) {
  field.set_stringvalue(value.hexString());
  return true;
}
MAP_API_REVISION_SET(sm::HashId) {
  field.set_stringvalue(value.hexString());
  return true;
}
MAP_API_REVISION_SET(int64_t) {
  field.set_longvalue(value);
  return true;
}
MAP_API_REVISION_SET(uint64_t) {
  field.set_ulongvalue(value);
  return true;
}
MAP_API_REVISION_SET(LogicalTime) {
  field.set_ulongvalue(value.serialize());
  return true;
}
MAP_API_REVISION_SET(Revision) {
  field.set_blobvalue(value.SerializeAsString());
  return true;
}
MAP_API_REVISION_SET(testBlob) {
  field.set_blobvalue(value.SerializeAsString());
  return true;
}
MAP_API_REVISION_SET(Poco::Data::BLOB) {
  field.set_blobvalue(value.rawContent(), value.size());
  return true;
}

/**
 * GET
 */
MAP_API_REVISION_GET(std::string) {
  *value = field.string_value();
  return true;
}
MAP_API_REVISION_GET(double) {  // NOLINT ("All parameters should be named ...")
  *value = field.double_value();
  return true;
}
MAP_API_REVISION_GET(int32_t) {
  *value = field.int_value();
  return true;
}
MAP_API_REVISION_GET(uint32_t) {
  *value = field.uint_value();
  return true;
}
MAP_API_REVISION_GET(Id) {
  if (!value->fromHexString(field.string_value())) {
    LOG(FATAL) << "Failed to parse Hash id from string \""
               << field.string_value() << "\" for field "
               << field.nametype().name();
  }
  return true;
}
MAP_API_REVISION_GET(bool) {  // NOLINT ("All parameters should be named ...")
  *value = field.int_value() != 0;
  return true;
}
MAP_API_REVISION_GET(sm::HashId) {
  if (!value->fromHexString(field.string_value())) {
    LOG(FATAL) << "Failed to parse Hash id from string \""
               << field.string_value() << "\" for field "
               << field.nametype().name();
  }
  return true;
}
MAP_API_REVISION_GET(int64_t) {
  *value = field.long_value();
  return true;
}
MAP_API_REVISION_GET(uint64_t) {
  *value = field.ulong_value();
  return true;
}
MAP_API_REVISION_GET(LogicalTime) {
  *value = LogicalTime(field.ulong_value());
  return true;
}
MAP_API_REVISION_GET(Revision) {
  bool parsed = value->ParseFromString(field.blob_value());
  if (!parsed) {
    LOG(FATAL) << "Failed to parse revision";
    return false;
  }
  return true;
}
MAP_API_REVISION_GET(testBlob) {
  bool parsed = value->ParseFromString(field.blob_value());
  if (!parsed) {
    LOG(FATAL) << "Failed to parse test blob";
    return false;
  }
  return true;
}
MAP_API_REVISION_GET(Poco::Data::BLOB) {
  *value = Poco::Data::BLOB(field.blob_value());
  return true;
}

} /* namespace map_api */
