#ifndef DMAP_REVISION_INL_H_
#define DMAP_REVISION_INL_H_

#include <string>

#include <glog/logging.h>

namespace dmap {

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
      << "Type mismatch when trying to get field " << index
      << ". May it be that you are using outdated save files?";
  return get(field, value);
}

template <typename FieldType>
bool Revision::set(proto::TableField* field, const FieldType& value) {
  return value.SerializeToString(CHECK_NOTNULL(field)->mutable_blob_value());
}

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

template <typename ExpectedType>
bool Revision::verifyEqual(int index, const ExpectedType& expected) const {
  ExpectedType value;
  get(index, &value);
  return value == expected;
}

DMAP_DECLARE_TYPE_SUPPORT(std::string);     // NOLINT
DMAP_DECLARE_TYPE_SUPPORT(double);          // NOLINT
DMAP_DECLARE_TYPE_SUPPORT(int32_t);         // NOLINT
DMAP_DECLARE_TYPE_SUPPORT(uint32_t);        // NOLINT
DMAP_DECLARE_TYPE_SUPPORT(common::Id);      // NOLINT
DMAP_DECLARE_TYPE_SUPPORT(bool);            // NOLINT
DMAP_DECLARE_TYPE_SUPPORT(aslam::HashId);   // NOLINT
DMAP_DECLARE_TYPE_SUPPORT(int64_t);         // NOLINT
DMAP_DECLARE_TYPE_SUPPORT(uint64_t);        // NOLINT
DMAP_DECLARE_TYPE_SUPPORT(LogicalTime);     // NOLINT
DMAP_DECLARE_TYPE_SUPPORT(Revision);        // NOLINT
DMAP_DECLARE_TYPE_SUPPORT(testBlob);        // NOLINT
DMAP_DECLARE_TYPE_SUPPORT(Revision::Blob);  // NOLINT

}  // namespace dmap

#endif  // DMAP_REVISION_INL_H_
