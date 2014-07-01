#include <Poco/Data/Common.h>
#include <Poco/Data/BLOB.h>

#include <map-api/id.h>
#include <map-api/revision.h>
#include <map-api/logical-time.h>

#include <glog/logging.h>

namespace map_api {

bool Revision::find(const std::string& name, proto::TableField** field){
  FieldMap::iterator find = fields_.find(name);
  if (find == fields_.end()) {
    LOG(ERROR) << "Attempted to access inexistent field " << name;
    return false;
  }
  *field = mutable_fieldqueries(find->second);
  return true;
}

bool Revision::find(const std::string& name, const proto::TableField** field)
const{
  FieldMap::const_iterator find = fields_.find(name);
  if (find == fields_.end()) {
    LOG(ERROR) << "Attempted to access inexistent field " << name;
    return false;
  }
  *field = &fieldqueries(find->second);
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
    case (proto::TableFieldDescriptor_Type_UINT64): {
      stat << fieldqueries(field).ulongvalue();
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

std::shared_ptr<Poco::Data::BLOB> Revision::insertPlaceHolder(
    const std::string& field, Poco::Data::Statement& stat) const {
  FieldMap::const_iterator fieldIt = fields_.find(field);
  CHECK(fieldIt != fields_.end()) << "Attempted to access inexisting field " <<
      field;
  return insertPlaceHolder(fieldIt->second, stat);
}

void Revision::addField(const proto::TableFieldDescriptor& descriptor){
  // add field
  *add_fieldqueries()->mutable_nametype() = descriptor;
  // add to index
  fields_[descriptor.name()] = fieldqueries_size() - 1;
}

bool Revision::structureMatch(const Revision& other) const {
  if (fields_.size() != other.fields_.size()){
    LOG(INFO) << "Field count does not match";
    return false;
  }
  FieldMap::const_iterator leftIterator = fields_.begin(),
      rightIterator = other.fields_.begin();
  while (leftIterator != fields_.end()){
    if (leftIterator->first != rightIterator->first){
      LOG(INFO) << "Field name mismatch: " << leftIterator->first << " vs " <<
          rightIterator->first;
      return false;
    }
    ++leftIterator;
    ++rightIterator;
  }
  return true;
}

bool Revision::ParseFromString(const std::string& data){
  bool success = proto::Revision::ParseFromString(data);
  CHECK(success) << "Parsing revision from string failed";
  for (int i = 0; i < fieldqueries_size(); ++i){
    fields_[fieldqueries(i).nametype().name()] = i;
  }
  return true;
}

std::string Revision::dumpToString() const {
  std::ostringstream dump_ss;
  dump_ss << "Table " << table() << ": {" << std::endl;
  for (const std::pair<const std::string, int>& name_field : fields_) {
    dump_ss << "\t" << name_field.first << ": ";
    const proto::TableField& field = fieldqueries(name_field.second);
    if (field.has_blobvalue()) dump_ss << field.blobvalue();
    if (field.has_doublevalue()) dump_ss << field.doublevalue();
    if (field.has_intvalue()) dump_ss << field.intvalue();
    if (field.has_longvalue()) dump_ss << field.longvalue();
    if (field.has_stringvalue()) dump_ss << field.stringvalue();
    dump_ss << std::endl;
  }
  dump_ss << "}" << std::endl;
  return dump_ss.str();
}

/**
 * PROTOBUFENUM
 */

REVISION_ENUM(std::string, proto::TableFieldDescriptor_Type_STRING);
REVISION_ENUM(double, proto::TableFieldDescriptor_Type_DOUBLE);
REVISION_ENUM(int32_t, proto::TableFieldDescriptor_Type_INT32);
REVISION_ENUM(Id, proto::TableFieldDescriptor_Type_HASH128);
REVISION_ENUM(int64_t, proto::TableFieldDescriptor_Type_INT64);
REVISION_ENUM(uint64_t, proto::TableFieldDescriptor_Type_UINT64);
REVISION_ENUM(LogicalTime, proto::TableFieldDescriptor_Type_UINT64);
REVISION_ENUM(Revision, proto::TableFieldDescriptor_Type_BLOB);
REVISION_ENUM(testBlob, proto::TableFieldDescriptor_Type_BLOB);
REVISION_ENUM(Poco::Data::BLOB, proto::TableFieldDescriptor_Type_BLOB);

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
REVISION_SET(Id){
  field.set_stringvalue(value.hexString());
  return true;
}
REVISION_SET(int64_t){
  field.set_longvalue(value);
  return true;
}
REVISION_SET(uint64_t){
  field.set_ulongvalue(value);
  return true;
}
REVISION_SET(LogicalTime){
  field.set_ulongvalue(value.serialize());
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
REVISION_GET(Id){
  if (!value->fromHexString(field.stringvalue())){
    LOG(FATAL) << "Failed to parse Hash id from string \"" <<
        field.stringvalue() << "\" for field " << field.nametype().name();
  }
  return true;
}
REVISION_GET(int64_t){
  *value = field.longvalue();
  return true;
}
REVISION_GET(uint64_t){
  *value = field.ulongvalue();
  return true;
}
REVISION_GET(LogicalTime){
  *value = LogicalTime(field.ulongvalue());
  return true;
}
REVISION_GET(Revision){
  bool parsed = value->ParseFromString(field.blobvalue());
  if (!parsed) {
    LOG(FATAL) << "Failed to parse revision";
    return false;
  }
  return true;
}
REVISION_GET(testBlob){
  bool parsed = value->ParseFromString(field.blobvalue());
  if (!parsed) {
    LOG(FATAL) << "Failed to parse test blob";
    return false;
  }
  return true;
}
REVISION_GET(Poco::Data::BLOB){
  *value = Poco::Data::BLOB(field.blobvalue());
  return true;
}

} /* namespace map_api */
