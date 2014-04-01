/*
 * table-field.h
 *
 *  Created on: Apr 1, 2014
 *      Author: titus
 */

#ifndef TABLE_FIELD_H_
#define TABLE_FIELD_H_

#include <Poco/Data/Statement.h>

#include "core.pb.h"

namespace map_api {

class TableField : public proto::TableField{
 public:
  /**
   * SQL type descriptor
   */
  const std::string sqlType() const;
  /**
   * Insert placeholder in SQLite insert statements
   */
  Poco::Data::Statement& insertPlaceHolder(Poco::Data::Statement& stat)
  const;
  /**
   * Sets field according to type.
   */
  template <typename FieldType>
  void set(const FieldType& value);
  /**
   * Gets field according to type.
   */
  template <typename FieldType>
  FieldType get() const;
};

} /* namespace map_api */

#endif /* TABLE_FIELD_H_ */
