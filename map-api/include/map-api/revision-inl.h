#ifndef MAP_API_REVISION_INL_H_
#define MAP_API_REVISION_INL_H_

#include <string>

#include <glog/logging.h>

namespace map_api {

template <typename FieldType>
void Revision::addField(int index) {
  addField(index, getProtobufTypeEnum<FieldType>());
}

template <typename FieldType>
bool Revision::set(int index, const FieldType& value) {
  CHECK_LT(index, underlying_revision_->custom_field_values_size())
      << "Index out of custom field bounds";
  proto::TableField* field =
      underlying_revision_->mutable_custom_field_values(index);
  CHECK_EQ(field->type(), getProtobufTypeEnum<FieldType>())
      << "Type mismatch when trying to set field " << index;
  return set(field, value);
}

template <typename FieldType>
bool Revision::get(int index, FieldType* value) const {
  CHECK_NOTNULL(value);
  CHECK_LT(index, underlying_revision_->custom_field_values_size())
      << "Index out of custom field bounds";
  const proto::TableField& field =
      underlying_revision_->custom_field_values(index);
  CHECK_EQ(field.type(), getProtobufTypeEnum<FieldType>())
      << "Type mismatch when trying to get field " << index <<
      ". May it be that you are using outdated save files?";
  return get(field, value);
}

template <typename FieldType>
bool Revision::set(proto::TableField* field, const FieldType& value) {
  CHECK_NOTNULL(field)->set_blob_value(value.SerializeAsString());
  return true;
}
MAP_API_REVISION_SET(std::string);     // NOLINT
MAP_API_REVISION_SET(double);          // NOLINT
MAP_API_REVISION_SET(int32_t);         // NOLINT
MAP_API_REVISION_SET(uint32_t);        // NOLINT
MAP_API_REVISION_SET(bool);            // NOLINT
MAP_API_REVISION_SET(common::Id);      // NOLINT
MAP_API_REVISION_SET(aslam::HashId);   // NOLINT
MAP_API_REVISION_SET(int64_t);         // NOLINT
MAP_API_REVISION_SET(uint64_t);        // NOLINT
MAP_API_REVISION_SET(LogicalTime);     // NOLINT
MAP_API_REVISION_SET(Revision);        // NOLINT
MAP_API_REVISION_SET(testBlob);        // NOLINT
MAP_API_REVISION_SET(Revision::Blob);  // NOLINT

template <typename FieldType>
bool Revision::get(const proto::TableField& field, FieldType* value) const {
  CHECK_NOTNULL(value);
  bool parsed = value->ParseFromString(field.blob_value());
  if (!parsed) {
    LOG(ERROR) << "Failed to parse protobuf.";
    return false;
  }
  return true;
}
MAP_API_REVISION_GET(std::string);     // NOLINT
MAP_API_REVISION_GET(double);          // NOLINT
MAP_API_REVISION_GET(int32_t);         // NOLINT
MAP_API_REVISION_GET(uint32_t);        // NOLINT
MAP_API_REVISION_GET(common::Id);      // NOLINT
MAP_API_REVISION_GET(bool);            // NOLINT
MAP_API_REVISION_GET(aslam::HashId);   // NOLINT
MAP_API_REVISION_GET(int64_t);         // NOLINT
MAP_API_REVISION_GET(uint64_t);        // NOLINT
MAP_API_REVISION_GET(LogicalTime);     // NOLINT
MAP_API_REVISION_GET(Revision);        // NOLINT
MAP_API_REVISION_GET(testBlob);        // NOLINT
MAP_API_REVISION_GET(Revision::Blob);  // NOLINT

template <typename ExpectedType>
bool Revision::verifyEqual(int index, const ExpectedType& expected) const {
  ExpectedType value;
  get(index, &value);
  return value == expected;
}

}  // namespace map_api

#endif  // MAP_API_REVISION_INL_H_
