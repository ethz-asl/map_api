#include <glog/logging.h>
#include <Poco/Data/Common.h>
#include <Poco/Data/BLOB.h>

#include <map-api/logical-time.h>
#include <map-api/revision.h>
#include <map-api/unique-id.h>

namespace map_api {

Revision::Revision(const std::shared_ptr<proto::Revision>& revision)
    : underlying_revision_(revision) {}

Revision::Revision(const Revision& other)
    : underlying_revision_(new proto::Revision(*other.underlying_revision_)) {}

std::shared_ptr<Poco::Data::BLOB> Revision::insertPlaceHolder(
    int index, Poco::Data::Statement* stat) const {
  CHECK_NOTNULL(stat);
  std::shared_ptr<Poco::Data::BLOB> blobPointer;
  const proto::TableField& field =
      underlying_revision_->custom_field_values(index);
  CHECK(field.has_type()) << "Trying to insert placeholder of undefined type";
  *stat << " ";
  switch (field.type()) {
    case proto::Type::BLOB: {
      blobPointer = std::make_shared<Poco::Data::BLOB>(
          Poco::Data::BLOB(field.blob_value()));
      *stat << "?", Poco::Data::use(*blobPointer);
      break;
    }
    case proto::Type::DOUBLE: {
      *stat << field.double_value();
      break;
    }
    case proto::Type::HASH128: {
      *stat << "?", Poco::Data::use(field.string_value());
      break;
    }
    case proto::Type::INT32: {
      *stat << field.int_value();
      break;
    }
    case(proto::Type::UINT32) : {
      *stat << field.unsigned_int_value();
      break;
    }
    case proto::Type::INT64: {
      *stat << field.long_value();
      break;
    }
    case proto::Type::UINT64: {
      *stat << field.unsigned_long_value();
      break;
    }
    case proto::Type::STRING: {
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

void Revision::addField(int index, proto::Type type) {
  CHECK_EQ(underlying_revision_->custom_field_values_size(), index)
      << "Custom fields must be added in-order!";
  underlying_revision_->add_custom_field_values()->set_type(type);
}

bool Revision::structureMatch(const Revision& other) const {
  if (underlying_revision_->custom_field_values_size() !=
      other.underlying_revision_->custom_field_values_size()) {
    LOG(ERROR) << "Field count does not match";
    return false;
  }
  for (int i = 0; i < underlying_revision_->custom_field_values_size(); ++i) {
    if (underlying_revision_->custom_field_values(i).type() !=
        other.underlying_revision_->custom_field_values(i).type()) {
      LOG(ERROR) << "Field type mismatch at position " << i;
      return false;
    }
  }
  return true;
}

bool Revision::fieldMatch(const Revision& other, int key) const {
  const proto::TableField& a = underlying_revision_->custom_field_values(key);
  const proto::TableField& b =
      other.underlying_revision_->custom_field_values(key);
  switch (a.type()) {
    case proto::Type::BLOB: { return a.blob_value() == b.blob_value(); }
    case(proto::Type::DOUBLE) : { return a.double_value() == b.double_value(); }
    case(proto::Type::HASH128) : {
      return a.string_value() == b.string_value();
    }
    case(proto::Type::INT32) : { return a.int_value() == b.int_value(); }
    case(proto::Type::UINT32) : {
      return a.unsigned_int_value() == b.unsigned_int_value();
    }
    case(proto::Type::INT64) : { return a.long_value() == b.long_value(); }
    case(proto::Type::UINT64) : {
      return a.unsigned_long_value() == b.unsigned_long_value();
    }
    case(proto::Type::STRING) : { return a.string_value() == b.string_value(); }
  }
  CHECK(false) << "Forgot switch case";
  return false;
}

std::string Revision::dumpToString() const {
  std::ostringstream dump_ss;
  dump_ss << "{" << std::endl;
  dump_ss << "\tid:" << getId<Id>() << std::endl;
  dump_ss << "\tinsert_time:" << getInsertTime() << std::endl;
  if (underlying_revision_->has_chunk_id()) {
    dump_ss << "\tchunk_id:" << getChunkId() << std::endl;
  }
  if (underlying_revision_->has_update_time()) {
    dump_ss << "\tupdate_time:" << getUpdateTime() << std::endl;
  }
  if (underlying_revision_->has_removed()) {
    dump_ss << "\tremoved:" << underlying_revision_->removed() << std::endl;
  }
  for (int i = 0; i < underlying_revision_->custom_field_values_size(); ++i) {
    dump_ss << "\t" << i << ": ";
    const proto::TableField& field =
        underlying_revision_->custom_field_values(i);
    if (field.has_blob_value()) dump_ss << field.blob_value();
    if (field.has_double_value()) dump_ss << field.double_value();
    if (field.has_int_value()) dump_ss << field.int_value();
    if (field.has_unsigned_int_value()) dump_ss << field.unsigned_int_value();
    if (field.has_long_value()) dump_ss << field.long_value();
    if (field.has_unsigned_long_value()) dump_ss << field.unsigned_long_value();
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
  field->set_string_value(value);
  return true;
}
MAP_API_REVISION_SET(double) {  // NOLINT ("All parameters should be named ...")
  field->set_double_value(value);
  return true;
}
MAP_API_REVISION_SET(int32_t) {
  field->set_int_value(value);
  return true;
}
MAP_API_REVISION_SET(uint32_t) {
  field->set_unsigned_int_value(value);
  return true;
}
MAP_API_REVISION_SET(bool) {  // NOLINT ("All parameters should be named ...")
  field->set_int_value(value ? 1 : 0);
  return true;
}
MAP_API_REVISION_SET(Id) {
  field->set_string_value(value.hexString());
  return true;
}
MAP_API_REVISION_SET(sm::HashId) {
  field->set_string_value(value.hexString());
  return true;
}
MAP_API_REVISION_SET(int64_t) {
  field->set_long_value(value);
  return true;
}
MAP_API_REVISION_SET(uint64_t) {
  field->set_unsigned_long_value(value);
  return true;
}
MAP_API_REVISION_SET(LogicalTime) {
  field->set_unsigned_long_value(value.serialize());
  return true;
}
MAP_API_REVISION_SET(Revision) {
  field->set_blob_value(value.serializeUnderlying());
  return true;
}
MAP_API_REVISION_SET(testBlob) {
  field->set_blob_value(value.SerializeAsString());
  return true;
}
MAP_API_REVISION_SET(Poco::Data::BLOB) {
  field->set_blob_value(value.rawContent(), value.size());
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
  *value = field.unsigned_int_value();
  return true;
}
MAP_API_REVISION_GET(Id) {
  if (!value->fromHexString(field.string_value())) {
    LOG(FATAL) << "Failed to parse Hash id from string \""
               << field.string_value() << "\"";
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
               << field.string_value() << "\"";
  }
  return true;
}
MAP_API_REVISION_GET(int64_t) {
  *value = field.long_value();
  return true;
}
MAP_API_REVISION_GET(uint64_t) {
  *value = field.unsigned_long_value();
  return true;
}
MAP_API_REVISION_GET(LogicalTime) {
  *value = LogicalTime(field.unsigned_long_value());
  return true;
}
MAP_API_REVISION_GET(Revision) {
  bool parsed =
      value->underlying_revision_->ParseFromString(field.blob_value());
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
