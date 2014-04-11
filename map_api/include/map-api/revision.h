/*
 * table-insert-query.h
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#ifndef REVISION_H_
#define REVISION_H_

#include <map>
#include <memory>

#include <Poco/Data/BLOB.h>
#include <Poco/Data/Statement.h>

#include "core.pb.h"

namespace map_api {

class Revision : public proto::Revision {
 public:
  /**
   * Insert placeholder in SQLite insert statements. Returns blob shared pointer
   * for dynamically created blob objects
   */
  std::shared_ptr<Poco::Data::BLOB>
  insertPlaceHolder(int field, Poco::Data::Statement& stat) const;

  /**
   * Gets protocol buffer enum for type
   */
  template <typename FieldType>
  static map_api::proto::TableFieldDescriptor_Type protobufEnum();
  /**
   * Supporting macro
   */
#define REVISION_ENUM(TYPE, ENUM) \
    template <> \
    map_api::proto::TableFieldDescriptor_Type \
    Revision::protobufEnum<TYPE>() { \
  return ENUM ; \
}

  /**
   * Sets field according to type.
   */
  template <typename FieldType>
  void set(const std::string& field, const FieldType& value);
  /**
   * Supporting macros
   */
#define REVISION_SET(TYPE) \
    template <> \
    void Revision::set<TYPE>(const std::string& field, const TYPE& value)
#define REVISION_TYPE_CHECK(TYPE)  \
    CHECK_EQ(find(field).nametype().type(), protobufEnum<TYPE>() ) << \
    "Trying to access non-" << #TYPE << " field"

  /**
   * Gets field according to type. Non-const because field lookup might lead
   * to re-indexing the map.
   */
  template <typename FieldType>
  FieldType get(const std::string& field);
  /**
   * Supporting macro
   */
#define REVISION_GET(TYPE) \
    template <> \
    TYPE Revision::get<TYPE>(const std::string& field)

 private:
  /**
   * A map of fields for more intuitive access.
   */
  typedef std::map<std::string, int> fieldMap;
  fieldMap fields_;
  bool index();
  /**
   * Access to the map. Non-const because might need to index.
   */
  proto::TableField& find(const std::string& field);

};

/**
 * A generic, blob-y field type for testing blob insertion
 */
class testBlob : public map_api::proto::TableField{
 public:
  inline bool operator==(const testBlob& other) const{
    if (!this->has_nametype())
      return !other.has_nametype();
    return nametype().name() == other.nametype().name();
  }
};

} /* namespace map_api */

#endif /* REVISION_H_ */
