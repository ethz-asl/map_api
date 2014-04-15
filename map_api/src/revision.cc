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

proto::TableField& Revision::find(const std::string& field){
  fieldMap::iterator find = fields_.find(field);
  // reindex if not found
  if (find == fields_.end()){
    index();
    find = fields_.find(field);
  }
  CHECK(find != fields_.end()) <<
      "Attempted to access inexistent field " << field;
  return *mutable_fieldqueries(find->second);
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
REVISION_ENUM(Poco::Data::BLOB, proto::TableFieldDescriptor_Type_BLOB)

/**
 * SET
 */
REVISION_SET(std::string){
  REVISION_TYPE_CHECK(std::string);
  find(field).set_stringvalue(value);
}
REVISION_SET(double){
  REVISION_TYPE_CHECK(double);
  find(field).set_doublevalue(value);
}
REVISION_SET(int32_t){
  REVISION_TYPE_CHECK(int32_t);
  find(field).set_intvalue(value);
}
REVISION_SET(Hash){
  REVISION_TYPE_CHECK(Hash);
  find(field).set_stringvalue(value.getString());
}
REVISION_SET(int64_t){
  REVISION_TYPE_CHECK(int64_t);
  find(field).set_longvalue(value);
}
REVISION_SET(Time){
  REVISION_TYPE_CHECK(Time);
  find(field).set_longvalue(value.serialize());
}
REVISION_SET(Revision){
  REVISION_TYPE_CHECK(Revision);
  find(field).set_blobvalue(value.SerializeAsString());
}
REVISION_SET(testBlob){
  REVISION_TYPE_CHECK(testBlob);
  find(field).set_blobvalue(value.SerializeAsString());
}
REVISION_SET(Poco::Data::BLOB){
  REVISION_TYPE_CHECK(Poco::Data::BLOB);
  find(field).set_blobvalue(value.rawContent(), value.size());
}

/**
 * GET
 */
REVISION_GET(std::string){
  REVISION_TYPE_CHECK(std::string);
  return find(field).stringvalue();
}
REVISION_GET(double){
  REVISION_TYPE_CHECK(double);
  return find(field).doublevalue();
}
REVISION_GET(int32_t){
  REVISION_TYPE_CHECK(int32_t);
  return find(field).intvalue();
}
REVISION_GET(Hash){
  REVISION_TYPE_CHECK(Hash);
  return map_api::Hash::cast(find(field).stringvalue());
}
REVISION_GET(int64_t){
  REVISION_TYPE_CHECK(int64_t);
  return find(field).longvalue();
}
REVISION_GET(Time){
  REVISION_TYPE_CHECK(Time);
  return Time(find(field).longvalue());
}
REVISION_GET(Revision){
  REVISION_TYPE_CHECK(Revision);
  Revision value;
  bool parsed = value.ParseFromString(find(field).blobvalue());
  CHECK(parsed) << "Failed to parse revision";
  return value;
}
REVISION_GET(testBlob){
  REVISION_TYPE_CHECK(testBlob);
  testBlob value;
  bool parsed = value.ParseFromString(find(field).blobvalue());
  CHECK(parsed) << "Failed to parse test blob";
  return value;
}
REVISION_GET(Poco::Data::BLOB){
  REVISION_TYPE_CHECK(Poco::Data::BLOB);
  return Poco::Data::BLOB(find(field).blobvalue());
}

} /* namespace map_api */
