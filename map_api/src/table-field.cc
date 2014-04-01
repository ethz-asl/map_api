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
  switch (nametype().type()){
    case (proto::TableFieldDescriptor_Type_BLOB): return stat << "?",
        Poco::Data::use(blobvalue());
    case (proto::TableFieldDescriptor_Type_DOUBLE): return stat << "?",
        Poco::Data::use(doublevalue());
    case (proto::TableFieldDescriptor_Type_HASH128): return stat << "?",
        Poco::Data::use(stringvalue());
    case (proto::TableFieldDescriptor_Type_INT32): return stat << "?",
        Poco::Data::use(intvalue());
    case (proto::TableFieldDescriptor_Type_INT64): return stat << "?",
        Poco::Data::use(longvalue());
    case (proto::TableFieldDescriptor_Type_STRING): return stat << "?",
        Poco::Data::use(stringvalue());
    default:
      LOG(FATAL) << "Field type not handled";
      return stat;
  }
}

/**
 * FUNCTIONS IMPLEMENTED WITH TEMPLATES
 */

template <>
void TableField::set<std::string>(const std::string& value){
  if (nametype().type() != proto::TableFieldDescriptor_Type_STRING){
    LOG(FATAL) << "Trying to set non-string field to string";
  }
  set_stringvalue(value);
}


template <>
std::string TableField::get<std::string>() const{
  if (nametype().type() != proto::TableFieldDescriptor_Type_STRING){
    LOG(FATAL) << "Trying to get string from non-string field";
  }
  return stringvalue();
}

} /* namespace map_api */
