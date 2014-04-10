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
   * Gets field according to type.
   */
  template <typename FieldType>
  FieldType get() const;

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
