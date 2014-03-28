/*
 * table-insert-query.cc
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#include <map-api/table-insert-query.h>

#include <glog/logging.h>

namespace map_api {

bool TableInsertQuery::index() {
  for (int i=0; i<this->fieldqueries_size(); ++i){
    fields_[this->fieldqueries(i).nametype().name()] =
        i;
  }
  return true;
}

proto::TableField* TableInsertQuery::operator[](std::string field){
  fieldMap::iterator find = fields_.find(field);
  if (find == fields_.end()){
    LOG(FATAL) << "Attempted to set inexistent field." << std::endl;
  }
  return this->mutable_fieldqueries(find->second);
}

} /* namespace map_api */
