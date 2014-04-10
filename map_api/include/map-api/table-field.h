/*
 * table-field.h
 *
 *  Created on: Apr 1, 2014
 *      Author: titus
 */

#ifndef TABLE_FIELD_H_
#define TABLE_FIELD_H_

#include <memory>

#include <Poco/Data/Statement.h>
#include <Poco/Data/BLOB.h>

#include "core.pb.h"

namespace map_api {

/**
 * IMPORTANT: May only introduce methods, but not properties, as subject to
 * static cast from proto::TableField
 */
class TableField : public proto::TableField{
 public:
  /**
   * SQL type descriptor
   */
  const std::string sqlType() const;
  /**
   * Insert placeholder in SQLite insert statements. Returns blob shared pointer
   * for dynimcally created blob objects
   */
  std::shared_ptr<Poco::Data::BLOB>
  insertPlaceHolder(Poco::Data::Statement& stat)
  const;
  /**
   * Sets field according to type. TODO(tcies) macros
   */
  template <typename FieldType>
  void set(const FieldType& value);
  /**
   * Gets field according to type.
   */
  template <typename FieldType>
  FieldType get() const;
  /**
   * Gets protocol buffer enum for type
   */
  template <typename FieldType>
  static map_api::proto::TableFieldDescriptor_Type protobufEnum();
};

class testBlob : public map_api::proto::TableField{
 public:
  inline bool operator==(const testBlob& other) const{
    if (!this->has_nametype())
      return !other.has_nametype();
    return nametype().name() == other.nametype().name();
  }
};

} /* namespace map_api */

#endif /* TABLE_FIELD_H_ */
