#include <map-api/revision.h>

#include <glog/logging.h>
#include <map-api/logical-time.h>
#include <map-api/net-table-manager.h>
#include <map-api/trackee-multimap.h>
#include <map-api-common/backtrace.h>
#include <map-api-common/unique-id.h>

namespace map_api {

void Revision::copyForWrite(std::shared_ptr<Revision>* result) const {
  CHECK_NOTNULL(result);
  std::shared_ptr<proto::Revision> copy(
      new proto::Revision(*underlying_revision_));
  return fromProto(copy, result);
}

void Revision::fromProto(const std::shared_ptr<proto::Revision>& revision_proto,
                         std::shared_ptr<Revision>* result) {
  CHECK_NOTNULL(result);
  // Because -> keeps dereferencing:
  // http://stackoverflow.com/questions/20583450/the-operator-return-value-of-smart-pointers/20583499#20583499
  std::shared_ptr<Revision>& deref_result = *result;
  deref_result.reset(new Revision);
  (*result)->underlying_revision_ = revision_proto;
}
void Revision::fromProto(const std::shared_ptr<proto::Revision>& revision_proto,
                         std::shared_ptr<const Revision>* result) {
  CHECK_NOTNULL(result);
  std::shared_ptr<Revision> non_const;
  fromProto(revision_proto, &non_const);
  *result = non_const;
}

std::shared_ptr<Revision> Revision::fromProtoString(
    const std::string& revision_proto_string) {
  std::shared_ptr<Revision> result(new Revision);
  result->underlying_revision_.reset(new proto::Revision);
  CHECK(result->underlying_revision_->ParseFromString(revision_proto_string));
  return result;
}

void Revision::addField(int index, proto::Type type) {
  CHECK_EQ(underlying_revision_->custom_field_values_size(), index)
      << "Custom fields must be added in-order!";
  underlying_revision_->add_custom_field_values()->set_type(type);
}
void Revision::removeLastField() {
  CHECK_GT(underlying_revision_->custom_field_values_size(), 0);
  underlying_revision_->mutable_custom_field_values()->RemoveLast();
}

bool Revision::hasField(int index) const {
  return index < underlying_revision_->custom_field_values_size();
}

proto::Type Revision::getFieldType(int index) const {
  return underlying_revision_->custom_field_values(index).type();
}

void Revision::clearCustomFieldValues() {
  for (proto::TableField& custom_field :
       *underlying_revision_->mutable_custom_field_values()) {
    proto::Type type = custom_field.type();
    custom_field.Clear();
    custom_field.set_type(type);
  }
}

bool Revision::operator==(const Revision& other) const {
  if (!structureMatch(other)) {
    return false;
  }
  if (other.getId<map_api_common::Id>() != getId<map_api_common::Id>()) {
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
  if (!areAllCustomFieldsEqual(other)) {
    return false;
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

bool Revision::areAllCustomFieldsEqual(const Revision& other) const {
  for (int i = 0; i < underlying_revision_->custom_field_values_size(); ++i) {
    if (!fieldMatch(other, i)) {
      VLOG(4) << "Custom field " << i << " diverges!";
      return false;
    }
  }
  return true;
}

std::string Revision::dumpToString() const {
  std::ostringstream dump_ss;
  dump_ss << "{" << std::endl;
  dump_ss << "\tid:" << getId<map_api_common::Id>() << std::endl;
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
  CHECK(underlying_revision_);
  CHECK_NOTNULL(result)->deserialize(*underlying_revision_);
}

bool Revision::fetchTrackedChunks() const {
  bool success = true;
  TrackeeMultimap trackee_multimap;
  getTrackedChunks(&trackee_multimap);
  LOG_IF(WARNING, trackee_multimap.empty())
      << "Fetch tracked chunks called, but no tracked chunks!";
  for (const TrackeeMultimap::value_type& table_trackees : trackee_multimap) {
    VLOG(3) << "Fetching " << table_trackees.second.size()
            << " tracked chunks from table " << table_trackees.first->name();
    if (!table_trackees.first->ensureHasChunks(table_trackees.second)) {
      success = false;
    }
    VLOG(3) << "Done.";
  }
  return success;
}

bool Revision::defaultAutoMergePolicy(const Revision& conflicting_revision,
                                      const Revision& original_revision,
                                      Revision* revision_at_hand) {
  CHECK_NOTNULL(revision_at_hand);
  const bool conflict_innovates =
      !conflicting_revision.areAllCustomFieldsEqual(original_revision);
  if (conflict_innovates) {
    const bool this_innovates =
        !revision_at_hand->areAllCustomFieldsEqual(original_revision);

    // When both versions innovate, we fail the transaction per default, even
    // if the innovations are equal. This makes it more easy to test proper
    // functioning of transactions.
    if (this_innovates) {
      VLOG(3) << "Custom fields innovated by both!";
      return false;
    } else {
      revision_at_hand->underlying_revision_->mutable_custom_field_values()
          ->CopyFrom(conflicting_revision.underlying_revision_
                         ->custom_field_values());
    }
  }

  map_api_common::Id id, conflicting_id;
  id.deserialize(revision_at_hand->underlying_revision_->id());
  conflicting_id.deserialize(conflicting_revision.underlying_revision_->id());
  CHECK_EQ(id, conflicting_id);
  CHECK_EQ(revision_at_hand->underlying_revision_->insert_time(),
           conflicting_revision.underlying_revision_->insert_time());
  CHECK(!revision_at_hand->underlying_revision_->removed());
  CHECK(!conflicting_revision.underlying_revision_->removed());
  id.deserialize(revision_at_hand->underlying_revision_->chunk_id());
  conflicting_id.deserialize(
      conflicting_revision.underlying_revision_->chunk_id());
  CHECK_EQ(id, conflicting_id);

  TrackeeMultimap tracked_chunks, conflicting_tracked_chunks;
  tracked_chunks.deserialize(*revision_at_hand->underlying_revision_);
  conflicting_tracked_chunks.deserialize(
      *conflicting_revision.underlying_revision_);

  if (tracked_chunks.merge(conflicting_tracked_chunks)) {
    tracked_chunks.serialize(revision_at_hand->underlying_revision_.get());
  }
  return true;
}

// TODO(tcies): This should probably be more involved. Currently it's not
// possible to merge chunk tracking and custom fields from a custom merge
// policy.
bool Revision::tryAutoMerge(
    const Revision& conflicting_revision, const Revision& original_revision,
    const std::vector<AutoMergePolicy>& custom_merge_policies) {
  for (const AutoMergePolicy& policy : custom_merge_policies) {
    if (policy(conflicting_revision, original_revision, this)) {
      return true;
    }
  }
  if (defaultAutoMergePolicy(conflicting_revision, original_revision, this)) {
    return true;
  }
  return false;
}

/**
 * PROTOBUFENUM
 */

DMAP_TYPE_ENUM(std::string, proto::Type::STRING);
DMAP_TYPE_ENUM(double, proto::Type::DOUBLE);
DMAP_TYPE_ENUM(int32_t, proto::Type::INT32);
DMAP_TYPE_ENUM(uint32_t, proto::Type::UINT32);
DMAP_TYPE_ENUM(bool, proto::Type::INT32);
DMAP_TYPE_ENUM(map_api_common::Id, proto::Type::HASH128);
DMAP_TYPE_ENUM(map_api_common::HashId, proto::Type::HASH128);
DMAP_TYPE_ENUM(int64_t, proto::Type::INT64);
DMAP_TYPE_ENUM(uint64_t, proto::Type::UINT64);
DMAP_TYPE_ENUM(LogicalTime, proto::Type::UINT64);
DMAP_TYPE_ENUM(Revision, proto::Type::BLOB);
DMAP_TYPE_ENUM(testBlob, proto::Type::BLOB);
DMAP_TYPE_ENUM(Revision::Blob, proto::Type::BLOB);

/**
 * SET
 */
DMAP_REVISION_SET(std::string /*value*/) {
  field->set_string_value(value);
  return true;
}
DMAP_REVISION_SET(double /*value*/) {
  field->set_double_value(value);
  return true;
}
DMAP_REVISION_SET(int32_t /*value*/) {
  field->set_int_value(value);
  return true;
}
DMAP_REVISION_SET(uint32_t /*value*/) {
  field->set_unsigned_int_value(value);
  return true;
}
DMAP_REVISION_SET(bool /*value*/) {
  field->set_int_value(value ? 1 : 0);
  return true;
}
DMAP_REVISION_SET(map_api_common::Id /*value*/) {
  field->set_string_value(value.hexString());
  return true;
}
DMAP_REVISION_SET(map_api_common::HashId /*value*/) {
  field->set_string_value(value.hexString());
  return true;
}
DMAP_REVISION_SET(int64_t /*value*/) {
  field->set_long_value(value);
  return true;
}
DMAP_REVISION_SET(uint64_t /*value*/) {
  field->set_unsigned_long_value(value);
  return true;
}
DMAP_REVISION_SET(LogicalTime /*value*/) {
  field->set_unsigned_long_value(value.serialize());
  return true;
}
DMAP_REVISION_SET(Revision /*value*/) {
  field->set_blob_value(value.serializeUnderlying());
  return true;
}
DMAP_REVISION_SET(testBlob /*value*/) {
  field->set_blob_value(value.SerializeAsString());
  return true;
}
DMAP_REVISION_SET(Revision::Blob /*value*/) {
  field->set_blob_value(value.data(), value.size());
  return true;
}

/**
 * GET
 */
DMAP_REVISION_GET(std::string /*value*/) {
  *value = field.string_value();
  return true;
}
DMAP_REVISION_GET(double /*value*/) {
  *value = field.double_value();
  return true;
}
DMAP_REVISION_GET(int32_t /*value*/) {
  *value = field.int_value();
  return true;
}
DMAP_REVISION_GET(uint32_t /*value*/) {
  *value = field.unsigned_int_value();
  return true;
}
DMAP_REVISION_GET(map_api_common::Id /*value*/) {
  if (!value->fromHexString(field.string_value())) {
    LOG(FATAL) << "Failed to parse Hash id from string \""
               << field.string_value() << "\"";
  }
  return true;
}
DMAP_REVISION_GET(bool /*value*/) {
  *value = field.int_value() != 0;
  return true;
}
DMAP_REVISION_GET(map_api_common::HashId /*value*/) {
  if (!value->fromHexString(field.string_value())) {
    LOG(FATAL) << "Failed to parse Hash id from string \""
               << field.string_value() << "\"";
  }
  return true;
}
DMAP_REVISION_GET(int64_t /*value*/) {
  *value = field.long_value();
  return true;
}
DMAP_REVISION_GET(uint64_t /*value*/) {
  *value = field.unsigned_long_value();
  return true;
}
DMAP_REVISION_GET(LogicalTime /*value*/) {
  *value = LogicalTime(field.unsigned_long_value());
  return true;
}
DMAP_REVISION_GET(Revision /*value*/) {
  bool parsed =
      value->underlying_revision_->ParseFromString(field.blob_value());
  if (!parsed) {
    LOG(FATAL) << "Failed to parse revision";
    return false;
  }
  return true;
}
DMAP_REVISION_GET(testBlob /*value*/) {
  bool parsed = value->ParseFromString(field.blob_value());
  if (!parsed) {
    LOG(FATAL) << "Failed to parse test blob";
    return false;
  }
  return true;
}
DMAP_REVISION_GET(Revision::Blob /*value*/) {
  value->resize(field.blob_value().length());
  memcpy(value->data(), field.blob_value().c_str(),
         field.blob_value().length());
  return true;
}

}  // namespace map_api
