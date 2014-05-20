#ifndef REVISION_INL_H_
#define REVISION_INL_H_

#include <glog/logging.h>

#include "map-api/revision.h"
#include "map-api/time.h"

namespace map_api {

template<typename FieldType>
void Revision::addField(const std::string& name) {
  proto::TableFieldDescriptor descriptor;
  descriptor.set_name(name);
  descriptor.set_type(Revision::protobufEnum<FieldType>());
  addField(descriptor);
}

template <typename FieldType>
bool Revision::set(const std::string& fieldName, const FieldType& value){
  // 1. Check if field exists
  proto::TableField* field;
  if (!find(fieldName, &field)){
    LOG(FATAL) << "Trying to set inexistent field " << fieldName;
  }
  // 2. Check type
  CHECK_EQ(field->nametype().type(), Revision::protobufEnum<FieldType>()) <<
      "Type mismatch when trying to set " << fieldName;
  // 3. Set
  return set(*field, value);
}

template <typename FieldType>
bool Revision::get(const std::string& fieldName, FieldType* value) const {
  // 1. Check if field exists
  const proto::TableField* field;
  if (!find(fieldName, &field)){
    LOG(FATAL) << "Trying to get inexistent field " << fieldName;
  }
  // 2. Check type
  CHECK_EQ(field->nametype().type(), Revision::protobufEnum<FieldType>()) <<
      "Type mismatch when trying to get " << fieldName;
  // 3. Get
  return get(*field, value);
}

template <typename ExpectedType>
bool Revision::verify(const std::string& fieldName,
                      const ExpectedType& expected) const {
  ExpectedType value;
  get(fieldName, &value);
  return value == expected;
}

} // namespace map_api



#endif /* REVISION_INL_H_ */
