/*
 * table-insert-query.cc
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#include <map-api/revision.h>

#include <glog/logging.h>

namespace map_api {

bool Revision::index() {
  for (int i = 0; i < this->fieldqueries_size(); ++i){
    fields_[this->fieldqueries(i).nametype().name()] =
        i;
  }
  return true;
}

TableField& Revision::operator[](const std::string& field){
  fieldMap::iterator find = fields_.find(field);
  CHECK(find != fields_.end()) << "Attempted to access inexistent field.";
  return static_cast<TableField&>(
      *this->mutable_fieldqueries(find->second));
}

const TableField& Revision::operator[](
    const std::string& field) const{
  fieldMap::const_iterator find = fields_.find(field);
  CHECK(find != fields_.end()) << "Attempted to access inexistent field.";
  return static_cast<const TableField&>(
      this->fieldqueries(find->second));
}

} /* namespace map_api */
