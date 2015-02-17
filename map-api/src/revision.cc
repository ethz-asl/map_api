#include <map-api/revision.h>

#include <glog/logging.h>
#include <map-api/logical-time.h>
#include <map-api/net-table-manager.h>
#include <map-api/trackee-multimap.h>
#include <multiagent-mapping-common/unique-id.h>

namespace map_api {

Revision::Revision(const std::shared_ptr<proto::Revision>& revision)
    : underlying_revision_(revision) {}

Revision::Revision(const Revision& other)
    : underlying_revision_(new proto::Revision(*other.underlying_revision_)) {}

void Revision::addField(int index, proto::Type type) {
  CHECK_EQ(underlying_revision_->custom_field_values_size(), index)
      << "Custom fields must be added in-order!";
  underlying_revision_->add_custom_field_values()->set_type(type);
}

bool Revision::hasField(int index) const {
  return index < underlying_revision_->custom_field_values_size();
}

proto::Type Revision::getFieldType(int index) const {
  return underlying_revision_->custom_field_values(index).type();
}

bool Revision::operator==(const Revision& other) const {
  if (!structureMatch(other)) {
    return false;
  }
  if (other.getId<common::Id>() != getId<common::Id>()) {
    return false;
  }
  if (other.getInsertTime() != getInsertTime()) {
    return false;
  }
  if (other.getUpdateTime() != getUpdateTime()) {
    return false;
  }
  if (other.isRemoved() != isRemoved()) {
    return false;
  }
  if (other.getChunkId() != getChunkId()) {
    return false;
  }
  // Check custom fields.
  int num_fields = underlying_revision_->custom_field_values_size();
  for (int i = 0; i < num_fields; ++i) {
    if (!fieldMatch(other, i)) {
      return false;
    }
  }
  return true;
}

bool Revision::structureMatch(const Revision& reference) const {
  int common_field_count =
      reference.underlying_revision_->custom_field_values_size();
  if (underlying_revision_->custom_field_values_size() <
      reference.underlying_revision_->custom_field_values_size()) {
    LOG_FIRST_N(WARNING, 10) << "Revision has less custom fields than "
                                "reference. Proceed with caution.";
    common_field_count = underlying_revision_->custom_field_values_size();
  }
  for (int i = 0; i < common_field_count; ++i) {
    if (underlying_revision_->custom_field_values(i).type() !=
        reference.underlying_revision_->custom_field_values(i).type()) {
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
  dump_ss << "\tid:" << getId<common::Id>() << std::endl;
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

void Revision::getTrackedChunks(TrackeeMultimap* result) const {
  CHECK_NOTNULL(result)->deserialize(*underlying_revision_);
}

bool Revision::fetchTrackedChunks() const {
  bool success = true;
  TrackeeMultimap trackee_multimap;
  getTrackedChunks(&trackee_multimap);
  for (const TrackeeMultimap::value_type& table_trackees : trackee_multimap) {
    for (const common::Id& chunk_id : table_trackees.second) {
      if (table_trackees.first->getChunk(chunk_id) == nullptr) {
        success = false;
      }
    }
  }
  return success;
}

/**
 * PROTOBUFENUM
 */

MAP_API_TYPE_ENUM(std::string, proto::Type::STRING);
MAP_API_TYPE_ENUM(double, proto::Type::DOUBLE);
MAP_API_TYPE_ENUM(int32_t, proto::Type::INT32);
MAP_API_TYPE_ENUM(uint32_t, proto::Type::UINT32);
MAP_API_TYPE_ENUM(bool, proto::Type::INT32);
MAP_API_TYPE_ENUM(common::Id, proto::Type::HASH128);
MAP_API_TYPE_ENUM(sm::HashId, proto::Type::HASH128);
MAP_API_TYPE_ENUM(int64_t, proto::Type::INT64);
MAP_API_TYPE_ENUM(uint64_t, proto::Type::UINT64);
MAP_API_TYPE_ENUM(LogicalTime, proto::Type::UINT64);
MAP_API_TYPE_ENUM(Revision, proto::Type::BLOB);
MAP_API_TYPE_ENUM(testBlob, proto::Type::BLOB);
MAP_API_TYPE_ENUM(Revision::Blob, proto::Type::BLOB);

/**
 * SET
 */
MAP_API_REVISION_SET(std::string /*value*/) {
  field->set_string_value(value);
  return true;
}
MAP_API_REVISION_SET(double /*value*/) {
  field->set_double_value(value);
  return true;
}
MAP_API_REVISION_SET(int32_t /*value*/) {
  field->set_int_value(value);
  return true;
}
MAP_API_REVISION_SET(uint32_t /*value*/) {
  field->set_unsigned_int_value(value);
  return true;
}
MAP_API_REVISION_SET(bool /*value*/) {
  field->set_int_value(value ? 1 : 0);
  return true;
}
MAP_API_REVISION_SET(common::Id /*value*/) {
  field->set_string_value(value.hexString());
  return true;
}
MAP_API_REVISION_SET(sm::HashId /*value*/) {
  field->set_string_value(value.hexString());
  return true;
}
MAP_API_REVISION_SET(int64_t /*value*/) {
  field->set_long_value(value);
  return true;
}
MAP_API_REVISION_SET(uint64_t /*value*/) {
  field->set_unsigned_long_value(value);
  return true;
}
MAP_API_REVISION_SET(LogicalTime /*value*/) {
  field->set_unsigned_long_value(value.serialize());
  return true;
}
MAP_API_REVISION_SET(Revision /*value*/) {
  field->set_blob_value(value.serializeUnderlying());
  return true;
}
MAP_API_REVISION_SET(testBlob /*value*/) {
  field->set_blob_value(value.SerializeAsString());
  return true;
}
MAP_API_REVISION_SET(Revision::Blob /*value*/) {
  field->set_blob_value(value.data(), value.size());
  return true;
}

/**
 * GET
 */
MAP_API_REVISION_GET(std::string /*value*/) {
  *value = field.string_value();
  return true;
}
MAP_API_REVISION_GET(double /*value*/) {
  *value = field.double_value();
  return true;
}
MAP_API_REVISION_GET(int32_t /*value*/) {
  *value = field.int_value();
  return true;
}
MAP_API_REVISION_GET(uint32_t /*value*/) {
  *value = field.unsigned_int_value();
  return true;
}
MAP_API_REVISION_GET(common::Id /*value*/) {
  if (!value->fromHexString(field.string_value())) {
    LOG(FATAL) << "Failed to parse Hash id from string \""
               << field.string_value() << "\"";
  }
  return true;
}
MAP_API_REVISION_GET(bool /*value*/) {
  *value = field.int_value() != 0;
  return true;
}
MAP_API_REVISION_GET(sm::HashId /*value*/) {
  if (!value->fromHexString(field.string_value())) {
    LOG(FATAL) << "Failed to parse Hash id from string \""
               << field.string_value() << "\"";
  }
  return true;
}
MAP_API_REVISION_GET(int64_t /*value*/) {
  *value = field.long_value();
  return true;
}
MAP_API_REVISION_GET(uint64_t /*value*/) {
  *value = field.unsigned_long_value();
  return true;
}
MAP_API_REVISION_GET(LogicalTime /*value*/) {
  *value = LogicalTime(field.unsigned_long_value());
  return true;
}
MAP_API_REVISION_GET(Revision /*value*/) {
  bool parsed =
      value->underlying_revision_->ParseFromString(field.blob_value());
  if (!parsed) {
    LOG(FATAL) << "Failed to parse revision";
    return false;
  }
  return true;
}
MAP_API_REVISION_GET(testBlob /*value*/) {
  bool parsed = value->ParseFromString(field.blob_value());
  if (!parsed) {
    LOG(FATAL) << "Failed to parse test blob";
    return false;
  }
  return true;
}
MAP_API_REVISION_GET(Revision::Blob /*value*/) {
  value->resize(field.blob_value().length());
  memcpy(value->data(), field.blob_value().c_str(),
         field.blob_value().length());
  return true;
}

}  // namespace map_api
