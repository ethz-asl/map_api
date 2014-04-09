/*
 * table-field.cc
 *
 *  Created on: Apr 1, 2014
 *      Author: titus
 */

#include <map-api/table-field.h>

#include <sstream>

#include <Poco/Data/Common.h>
#include <glog/logging.h>

#include <map-api/hash.h>
#include <map-api/time.h>

namespace map_api {

/**
 * FUNCTIONS IMPLEMENTED WITH SWITCH STATEMENTS
 */
const std::string TableField::sqlType() const{
  if (!nametype().has_type()){
    LOG(FATAL) << "Trying to get SQL type from field of undefined type";
    return "";
  }
  switch (nametype().type()){
    case (proto::TableFieldDescriptor_Type_BLOB): return "BLOB";
    case (proto::TableFieldDescriptor_Type_DOUBLE): return "REAL";
    case (proto::TableFieldDescriptor_Type_HASH128): return "TEXT";
    case (proto::TableFieldDescriptor_Type_INT32): return "INTEGER";
    case (proto::TableFieldDescriptor_Type_INT64): return "INTEGER";
    case (proto::TableFieldDescriptor_Type_STRING): return "TEXT";
    default:
      LOG(FATAL) << "Field type not handled";
      return "";
  }
}

Poco::Data::Statement& TableField::insertPlaceHolder(
    Poco::Data::Statement& stat) const{
  if (!nametype().has_type()){
    LOG(FATAL) << "Trying to insert placeholder of undefined type";
    return stat;
  }
  stat << " ";
  switch (nametype().type()){
    case (proto::TableFieldDescriptor_Type_BLOB):{
      stat << "?", Poco::Data::use(blobvalue());
      break;
    }
    case (proto::TableFieldDescriptor_Type_DOUBLE): {
      stat << doublevalue();
      break;
    }
    case (proto::TableFieldDescriptor_Type_HASH128): {
      stat << "?", Poco::Data::use(stringvalue());
      break;
    }
    case (proto::TableFieldDescriptor_Type_INT32): {
      stat << intvalue();
      break;
    }
    case (proto::TableFieldDescriptor_Type_INT64): {
      stat << longvalue();
      break;
    }
    case (proto::TableFieldDescriptor_Type_STRING): {
      stat << "?",    Poco::Data::use(stringvalue());
      break;
    }
    default:
      LOG(FATAL) << "Field type not handled";
      return stat;
  }
  return stat << " ";
}

/**
 * FUNCTIONS IMPLEMENTED WITH TEMPLATES
 */

template <>
void TableField::set<std::string>(const std::string& value){
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_STRING) <<
      "Trying to set non-string field to string";
  set_stringvalue(value);
}
template <>
void TableField::set<double>(const double& value){
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_DOUBLE) <<
      "Trying to set non-double field to double";
  set_doublevalue(value);
}
template <>
void TableField::set<int32_t>(const int32_t& value){
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_INT32) <<
      "Trying to set non-32-integer field to 32-integer";
  set_intvalue(value);
}
template <>
void TableField::set<map_api::Hash>(const map_api::Hash& value){
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_HASH128) <<
      "Trying to set non-hash field to hash";
  set_stringvalue(value.getString());
}
template <>
void TableField::set<int64_t>(const int64_t& value){
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_INT64) <<
      "Trying to set non-64-integer field to 64-integer";
  set_longvalue(value);
}
template <>
void TableField::set<Time>(const Time& value){
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_INT64) <<
      "Trying to set non-time field to time";
  set_longvalue(value.serialize());
}
template <>
void TableField::set<testBlob>(const testBlob& value){
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_BLOB) <<
      "Trying to set non-blob field to blob";
  set_blobvalue(value.SerializeAsString());
}


template <>
std::string TableField::get<std::string>() const{
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_STRING) <<
      "Trying to get string from non-string field";
  return stringvalue();
}
template <>
double TableField::get<double>() const{
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_DOUBLE) <<
      "Trying to get double from non-double field";
  return doublevalue();
}
template <>
int32_t TableField::get<int32_t>() const{
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_INT32) <<
      "Trying to get 32-integer from non-32-integer field";
  return intvalue();
}
template <>
map_api::Hash TableField::get<Hash>() const{
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_HASH128) <<
      "Trying to get hash from non-hash field";
  return map_api::Hash::cast(stringvalue());
}
template <>
int64_t TableField::get<int64_t>() const{
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_INT64) <<
      "Trying to get 64-integer from non-64-integer field";
  return longvalue();
}
template <>
Time TableField::get<Time>() const{
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_INT64) <<
      "Trying to get time from non-time field";
  return Time(longvalue());
}
template <>
testBlob TableField::get<testBlob>() const{
  CHECK_EQ(nametype().type(), proto::TableFieldDescriptor_Type_BLOB) <<
      "Trying to get blob from non-blob field";
  testBlob field;
  field.ParseFromString(blobvalue());
  return field;
}


template <>
map_api::proto::TableFieldDescriptor_Type
TableField::protobufEnum<std::string>(){
  return proto::TableFieldDescriptor_Type_STRING;
}
template <>
map_api::proto::TableFieldDescriptor_Type
TableField::protobufEnum<double>(){
  return proto::TableFieldDescriptor_Type_DOUBLE;
}
template <>
map_api::proto::TableFieldDescriptor_Type
TableField::protobufEnum<int32_t>(){
  return proto::TableFieldDescriptor_Type_INT32;
}
template <>
map_api::proto::TableFieldDescriptor_Type
TableField::protobufEnum<map_api::Hash>(){
  return proto::TableFieldDescriptor_Type_HASH128;
}
template <>
map_api::proto::TableFieldDescriptor_Type
TableField::protobufEnum<int64_t>(){
  return proto::TableFieldDescriptor_Type_INT64;
}
template <>
map_api::proto::TableFieldDescriptor_Type
TableField::protobufEnum<Time>(){
  return proto::TableFieldDescriptor_Type_INT64;
}
template <>
map_api::proto::TableFieldDescriptor_Type
TableField::protobufEnum<testBlob>(){
  return proto::TableFieldDescriptor_Type_BLOB;
}

} /* namespace map_api */
