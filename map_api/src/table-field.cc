/*
 * table-field.cc
 *
 *  Created on: Apr 1, 2014
 *      Author: titus
 */

#include <map-api/table-field.h>

#include <sstream>
#include <vector>

#include <Poco/Data/Common.h>
#include <Poco/Data/BLOB.h>

#include <glog/logging.h>

#include <map-api/hash.h>
#include <map-api/time.h>
#include <map-api/revision.h>

namespace map_api {

/**
 * FUNCTIONS IMPLEMENTED WITH TEMPLATES
 */



/**
 * GET
 */

template <>
std::string TableField::get<std::string>() const {
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_STRING) <<
      "Trying to get string from non-string field";
  return stringvalue();
}
template <>
double TableField::get<double>() const {
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_DOUBLE) <<
      "Trying to get double from non-double field";
  return doublevalue();
}
template <>
int32_t TableField::get<int32_t>() const {
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_INT32) <<
      "Trying to get 32-integer from non-32-integer field";
  return intvalue();
}
template <>
map_api::Hash TableField::get<Hash>() const {
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_HASH128) <<
      "Trying to get hash from non-hash field";
  return map_api::Hash::cast(stringvalue());
}
template <>
int64_t TableField::get<int64_t>() const {
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_INT64) <<
      "Trying to get 64-integer from non-64-integer field";
  return longvalue();
}
template <>
Time TableField::get<Time>() const {
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_INT64) <<
      "Trying to get time from non-time field";
  return Time(longvalue());
}
template <>
Revision TableField::get<Revision>() const {
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_BLOB) <<
      "Trying to get revision from non-revision field";
  Revision field;
  bool parsed = field.ParseFromString(blobvalue());
  CHECK(parsed) << "Failed to parse revision";
  return field;
}
template <>
testBlob TableField::get<testBlob>() const {
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_BLOB) <<
      "Trying to get blob from non-blob field";
  testBlob field;
  bool parsed = field.ParseFromString(blobvalue());
  CHECK(parsed) << "Failed to parse testBlob";
  return field;
}



} /* namespace map_api */
