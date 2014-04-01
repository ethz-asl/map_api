/*
 * table-insert-query.h
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#ifndef TABLE_INSERT_QUERY_H_
#define TABLE_INSERT_QUERY_H_

#include <map>

#include <map-api/table-field.h>
#include "core.pb.h"

namespace map_api {

class TableInsertQuery : public proto::TableInsertQuery {
 public:
  bool index();
  TableField& operator[](std::string field);
 private:
  /**
   * A map of fields for more intuitive access.
   */
  typedef std::map<std::string, int> fieldMap;
   fieldMap fields_;
};

} /* namespace map_api */

#endif /* TABLE_INSERT_QUERY_H_ */
