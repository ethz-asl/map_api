#ifndef MAP_API_REVISION_INL_H_
#define MAP_API_REVISION_INL_H_

#include <glog/logging.h>

#include "map-api/revision.h"
#include "map-api/logical-time.h"

namespace map_api {

template <typename FieldType>
void Revision::addField(int index) {
  addField(index, getProtobufTypeEnum<FieldType>());
}

template <typename FieldType>
bool Revision::set(int index, const FieldType& value) {
  CHECK_LE(index, underlying_revision_->custom_field_values_size())
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
  CHECK_LE(index, underlying_revision_->custom_field_values_size())
      << "Index out of custom field bounds";
  const proto::TableField& field =
      underlying_revision_->custom_field_values(index);
  CHECK_EQ(field.type(), getProtobufTypeEnum<FieldType>())
      << "Type mismatch when trying to get field " << index <<
      ". May it be that you are using outdated save files?";
  return get(field, value);
}

template <typename ExpectedType>
bool Revision::verifyEqual(int index, const ExpectedType& expected) const {
  ExpectedType value;
  get(index, &value);
  return value == expected;
}

}  // namespace map_api

#endif  // MAP_API_REVISION_INL_H_
