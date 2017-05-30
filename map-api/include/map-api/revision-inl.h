// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

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

MAP_API_DECLARE_TYPE_SUPPORT(std::string);     // NOLINT
MAP_API_DECLARE_TYPE_SUPPORT(double);          // NOLINT
MAP_API_DECLARE_TYPE_SUPPORT(int32_t);         // NOLINT
MAP_API_DECLARE_TYPE_SUPPORT(uint32_t);        // NOLINT
MAP_API_DECLARE_TYPE_SUPPORT(map_api_common::Id); // NOLINT
MAP_API_DECLARE_TYPE_SUPPORT(bool);            // NOLINT
MAP_API_DECLARE_TYPE_SUPPORT(map_api_common::HashId); // NOLINT
MAP_API_DECLARE_TYPE_SUPPORT(int64_t);         // NOLINT
MAP_API_DECLARE_TYPE_SUPPORT(uint64_t);        // NOLINT
MAP_API_DECLARE_TYPE_SUPPORT(LogicalTime);     // NOLINT
MAP_API_DECLARE_TYPE_SUPPORT(Revision);        // NOLINT
MAP_API_DECLARE_TYPE_SUPPORT(testBlob);        // NOLINT
MAP_API_DECLARE_TYPE_SUPPORT(Revision::Blob);  // NOLINT

}  // namespace map_api

#endif  // MAP_API_REVISION_INL_H_
