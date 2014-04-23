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

bool Revision::find(const std::string& name, proto::TableField** field){
  FieldMap::iterator find = fields_.find(name);
  // reindex if not found
  if (find == fields_.end()){
    index();
    find = fields_.find(name);
  }
  if (find == fields_.end()) {
    LOG(ERROR) << "Attempted to access inexistent field " << name;
    return false;
  }
  *field = mutable_fieldqueries(find->second);
  return true;
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

bool Revision::structureMatch(Revision& other){
  index();
  other.index();
  if (fields_.size() != other.fields_.size()){
    LOG(INFO) << "Field count does not match";
    return false;
  }
  FieldMap::iterator leftIterator = fields_.begin(),
      rightIterator = other.fields_.begin();
  while (leftIterator != fields_.end()){
    if (leftIterator->first != rightIterator->first){
      LOG(INFO) << "Field name mismatch: " << leftIterator->first << " vs " <<
          rightIterator->first;
      return false;
    }
    leftIterator++;
    rightIterator++;
  }
  return true;
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
  field.set_stringvalue(value);
  return true;
}
REVISION_SET(double){
  field.set_doublevalue(value);
  return true;
}
REVISION_SET(int32_t){
  field.set_intvalue(value);
  return true;
}
REVISION_SET(Hash){
  field.set_stringvalue(value.getString());
  return true;
}
REVISION_SET(int64_t){
  field.set_longvalue(value);
  return true;
}
REVISION_SET(Time){
  field.set_longvalue(value.serialize());
  return true;
}
REVISION_SET(Revision){
  field.set_blobvalue(value.SerializeAsString());
  return true;
}
REVISION_SET(testBlob){
  field.set_blobvalue(value.SerializeAsString());
  return true;
}
REVISION_SET(Poco::Data::BLOB){
  field.set_blobvalue(value.rawContent(), value.size());
  return true;
}

/**
 * GET
 */
REVISION_GET(std::string){
  *value = field.stringvalue();
  return true;
}
REVISION_GET(double){
  *value = field.doublevalue();
  return true;
}
REVISION_GET(int32_t){
  *value = field.intvalue();
  return true;
}
REVISION_GET(Hash){
  *value = map_api::Hash::cast(field.stringvalue());
  return true;
}
REVISION_GET(int64_t){
  *value = field.longvalue();
  return true;
}
REVISION_GET(Time){
  *value = Time(field.longvalue());
  return true;
}
REVISION_GET(Revision){
  bool parsed = value->ParseFromString(field.blobvalue());
  if (!parsed) {
    LOG(ERROR) << "Failed to parse revision";
    return false;
  }
  return true;
}
REVISION_GET(testBlob){
  bool parsed = value->ParseFromString(field.blobvalue());
  if (!parsed) {
    LOG(ERROR) << "Failed to parse test blob";
    return false;
  }
  return true;
}
REVISION_GET(Poco::Data::BLOB){
  *value = Poco::Data::BLOB(field.blobvalue());
  return true;
}

} /* namespace map_api */
