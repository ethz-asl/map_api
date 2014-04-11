/*
 * table-insert-query.cc
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#include <map-api/revision.h>
#include <map-api/hash.h>
#include <map-api/time.h>

#include <Poco/Data/Common.h>
#include <Poco/Data/BLOB.h>

#include <glog/logging.h>

namespace map_api {

bool Revision::index() {
  for (int i = 0; i < this->fieldqueries_size(); ++i){
    fields_[this->fieldqueries(i).nametype().name()] =
        i;
  }
  return true;
}

TableField& Revision::operator[](const std::string& field){
  fieldMap::iterator find = fields_.find(field);
  CHECK(find != fields_.end()) << "Attempted to access inexistent field.";
  return static_cast<TableField&>(
      *this->mutable_fieldqueries(find->second));
}

const TableField& Revision::operator[](
    const std::string& field) const{
  fieldMap::const_iterator find = fields_.find(field);
  CHECK(find != fields_.end()) << "Attempted to access inexistent field.";
  return static_cast<const TableField&>(
      this->fieldqueries(find->second));
}

std::shared_ptr<Poco::Data::BLOB> Revision::insertPlaceHolder(
    int field, Poco::Data::Statement& stat) const {
  std::shared_ptr<Poco::Data::BLOB> blobPointer;
  if (!fieldqueries(field).nametype().has_type()){
    LOG(FATAL) << "Trying to insert placeholder of undefined type";
    return blobPointer;
  }
  stat << " ";
  switch (fieldqueries(field).nametype().type()){
    case (proto::TableFieldDescriptor_Type_BLOB):{
      blobPointer = std::make_shared<Poco::Data::BLOB>(
          Poco::Data::BLOB(fieldqueries(field).blobvalue()));
      stat << "?", Poco::Data::use(*blobPointer);
      break;
    }
    case (proto::TableFieldDescriptor_Type_DOUBLE): {
      stat << fieldqueries(field).doublevalue();
      break;
    }
    case (proto::TableFieldDescriptor_Type_HASH128): {
      stat << "?", Poco::Data::use(fieldqueries(field).stringvalue());
      break;
    }
    case (proto::TableFieldDescriptor_Type_INT32): {
      stat << fieldqueries(field).intvalue();
      break;
    }
    case (proto::TableFieldDescriptor_Type_INT64): {
      stat << fieldqueries(field).longvalue();
      break;
    }
    case (proto::TableFieldDescriptor_Type_STRING): {
      stat << "?",    Poco::Data::use(fieldqueries(field).stringvalue());
      break;
    }
    default:
      LOG(FATAL) << "Field type not handled";
      return blobPointer;
  }
  stat << " ";
  return blobPointer;
}

/**
 * PROTOBUFENUM
 */

REVISION_ENUM(std::string, proto::TableFieldDescriptor_Type_STRING)
REVISION_ENUM(double, proto::TableFieldDescriptor_Type_DOUBLE)
REVISION_ENUM(int32_t, proto::TableFieldDescriptor_Type_INT32)
REVISION_ENUM(Hash, proto::TableFieldDescriptor_Type_HASH128)
REVISION_ENUM(int64_t, proto::TableFieldDescriptor_Type_INT64)
REVISION_ENUM(Time, proto::TableFieldDescriptor_Type_INT64)
REVISION_ENUM(Revision, proto::TableFieldDescriptor_Type_BLOB)
REVISION_ENUM(testBlob, proto::TableFieldDescriptor_Type_BLOB)

/**
 * SET
 */

// TODO(discuss) can this be slimmed down even further? I.e. check is always
// done, (*this)[field] could be turned into a reference.

REVISION_SET(std::string){
  REVISION_TYPE_CHECK(std::string);
  (*this)[field].set_stringvalue(value);
}
REVISION_SET(double){
  REVISION_TYPE_CHECK(double);
  (*this)[field].set_doublevalue(value);
}
REVISION_SET(int32_t){
  REVISION_TYPE_CHECK(int32_t);
  (*this)[field].set_intvalue(value);
}
REVISION_SET(Hash){
  REVISION_TYPE_CHECK(Hash);
  (*this)[field].set_stringvalue(value.getString());
}
REVISION_SET(int64_t){
  REVISION_TYPE_CHECK(int64_t);
  (*this)[field].set_longvalue(value);
}
REVISION_SET(Time){
  REVISION_TYPE_CHECK(Time);
  (*this)[field].set_longvalue(value.serialize());
}
REVISION_SET(Revision){
  REVISION_TYPE_CHECK(Revision);
  (*this)[field].set_blobvalue(value.SerializeAsString());
}
REVISION_SET(testBlob){
  REVISION_TYPE_CHECK(testBlob);
  (*this)[field].set_blobvalue(value.SerializeAsString());
}

} /* namespace map_api */
