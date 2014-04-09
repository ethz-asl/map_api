/*
 * history.cc
 *
 *  Created on: Apr 4, 2014
 *      Author: titus
 */

#include <map-api/history.h>

namespace map_api {

History::History(const CRUTableInterface& table) : table_(table){}

bool History::init(){
  return setup(table_.name() + "_history");
}

bool History::define(){
  addField("rowId",proto::TableFieldDescriptor_Type_HASH128);
  addField("previous",proto::TableFieldDescriptor_Type_HASH128);
  addField("revision",proto::TableFieldDescriptor_Type_STRING);
  addField("time",TableField::protobufEnum<Time>());
  return true;
}

Hash History::insert(const Revision& revision, const Hash& previous){
  std::shared_ptr<Revision> query = getTemplate();
  (*query)["rowId"].set(revision["ID"].get<Hash>());
  (*query)["previous"].set(previous);
  (*query)["revision"].set(revision.SerializeAsString());
  (*query)["time"].set(Time());
  return insertQuery(*query);
}

} /* namespace map_api */
