/*
 * table-insert-query.h
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#ifndef REVISION_H_
#define REVISION_H_

#include <map>

#include <map-api/table-field.h>
#include "core.pb.h"

namespace map_api {

class Revision : public proto::Revision {
 public:
  bool index();
  TableField& operator[](const std::string& field);
  const TableField& operator[](const std::string& field) const;
 private:
  /**
   * A map of fields for more intuitive access.
   */
  typedef std::map<std::string, int> fieldMap;
   fieldMap fields_;
};

} /* namespace map_api */

#endif /* REVISION_H_ */
