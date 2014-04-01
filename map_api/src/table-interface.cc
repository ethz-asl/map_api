/*
 * TableInterface.cpp
 *
 *  Created on: Mar 6, 2014
 *      Author: titus
 */

#include "map-api/table-interface.h"

#include <cstdio>
#include <map>

#include <Poco/Data/Common.h>
#include <Poco/Data/Statement.h>
#include <Poco/Data/SQLite/Connector.h>
#include <Poco/Data/BLOB.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "map-api/map-api-core.h"
#include "map-api/table-field.h"
#include "core.pb.h"

DEFINE_string(ipPort, "127.0.0.1:5050", "Define node ip and port");

namespace map_api {

bool TableInterface::define(){
  return true;
}

bool TableInterface::addField(std::string name,
                              proto::TableFieldDescriptor_Type type){
  // make sure the field has not been defined yet
  for (int i=0; i<fields_size(); ++i){
    if (fields(i).name().compare(name) == 0){
      LOG(FATAL) << "In table " << this->name() << ": Field " << name <<
          " defined twice!" << std::endl;
    }
  }
  // otherwise, proceed with adding field
  proto::TableFieldDescriptor *field = add_fields();
  field->set_name(name);
  field->set_type(type);
  return true;
}

bool TableInterface::setup(std::string name){
  // TODO(tcies) Test before initialized or RAII
  // TODO(tcies) check whether string safe for SQL, e.g. no hyphens
  set_name(name);
  // Define table fields
  // enforced fields id (hash) and owner
  addField("ID",proto::TableFieldDescriptor_Type_HASH128);
  addField("owner",proto::TableFieldDescriptor_Type_STRING);
  // user-defined fields
  define();

  // start up core if not running yet
  if (!MapApiCore::getInstance().isInitialized()) {
    MapApiCore::getInstance().init(FLAGS_ipPort);
  }
  // connect to database & create table
  session_ = MapApiCore::getInstance().getSession();
  createQuery();

  // Sync with cluster TODO(tcies)
  // sync();
  return true;
}

std::shared_ptr<TableInsertQuery> TableInterface::getTemplate() const{
  std::shared_ptr<TableInsertQuery> ret =
      std::shared_ptr<TableInsertQuery>(
          new TableInsertQuery);
  // add own name
  ret->set_target(name());
  // add editable fields
  for (int i=0; i<this->fields_size(); ++i){
    *(ret->add_fieldqueries()->mutable_nametype()) = this->fields(i);
  }
  ret->index();
  return ret;
}

bool TableInterface::createQuery(){
  Poco::Data::Statement stat(*session_);
  stat << "CREATE TABLE IF NOT EXISTS " << name() << " (";
  // parse fields from descriptor as database fields
  for (int i=0; i<this->fields_size(); ++i){
    const proto::TableFieldDescriptor &fieldDescriptor = this->fields(i);
    TableField field;
    // TODO(tcies) The following is specified in protobuf but not available
    // are we using an outdated version of protobuf?
    // field.set_allocated_nametype(&fieldDescriptor);
    *field.mutable_nametype() = fieldDescriptor;
    if (i != 0){
      stat << ", ";
    }
    stat << fieldDescriptor.name() << " " << field.sqlType();
    if (fieldDescriptor.name().compare("ID") == 0){
      stat << " PRIMARY KEY";
    }
  }
  stat << ");";
  stat.execute();
  return true;
}

map_api::Hash TableInterface::insertQuery(TableInsertQuery& query){
  // set ID (TODO(tcies): set owner as well)
  map_api::Hash idHash(query.SerializeAsString());
  query["ID"].set_stringvalue(idHash.getString());

  // assemble SQLite statement
  Poco::Data::Statement stat(*session_);
  // NB: sqlite placeholders work only for column values
  stat << "INSERT INTO " << name() << " ";

  stat << "(";
  for (int i=0; i<query.fieldqueries_size(); ++i){
    if (i>0){
      stat << ", ";
    }
    const TableField& field = static_cast<const TableField&>(
        query.fieldqueries(i));
    stat << field.nametype().name();
  }
  stat << ") VALUES ( ";
  for (int i=0; i<query.fieldqueries_size(); ++i){
    if (i>0){
      stat << " , ";
    }
    const TableField& field = static_cast<const TableField&>(
        query.fieldqueries(i));
    field.insertPlaceHolder(stat);
  }
  stat << " );";

  try {
    stat.execute();
  }
  catch(std::exception &e){
    LOG(FATAL) << "Insert failed with exception " << e.what();
  }

  // TODO (tcies) trigger subscribers
  return idHash;
}

std::shared_ptr<TableInsertQuery> TableInterface::getRow(
    const map_api::Hash &id) const{
  std::shared_ptr<TableInsertQuery> query = getTemplate();
  Poco::Data::Statement stat(*session_);
  stat << "SELECT ";

  // because protobuf won't supply mutable pointers to numeric values, we can't
  // pass them as reference to the into() binding of Poco::Data; they have to
  // be assigned a posteriori - this is done through this map
  std::map<std::string, double> doublePostApply;

  for (int i=0; i<query->fieldqueries_size(); ++i){
    if (i>0){
      stat << ", ";
    }
    proto::TableField& field = *query->mutable_fieldqueries(i);
    stat << field.nametype().name();
    // TODO(simon) do you see a reasonable way to move this to TableField?
    switch(field.nametype().type()){
      case (proto::TableFieldDescriptor_Type_BLOB):{
        stat, Poco::Data::into(*field.mutable_blobvalue());
        break;
      }
      case (proto::TableFieldDescriptor_Type_DOUBLE):{
        stat, Poco::Data::into(doublePostApply[field.nametype().name()]);
        break;
      }
      case (proto::TableFieldDescriptor_Type_STRING): // Fallthrough intended
      case (proto::TableFieldDescriptor_Type_HASH128):{
        stat, Poco::Data::into(*field.mutable_stringvalue());
        break;
      }
      default:{
        LOG(FATAL) << "Type of field supplied to select query unknown" <<
            std::endl;
      }
    }
  }

  stat << " FROM " << name() << " WHERE ID LIKE :id",
      Poco::Data::use(id.getString());

  try{
    stat.execute();
  }
  catch (std::exception& e){
    LOG(ERROR) << "Row " << id.getString() << " not found!";
  return std::shared_ptr<TableInsertQuery>();
  }

  // write values that couldn't be written directly
  for (std::pair<std::string, double> fieldDouble : doublePostApply){
    (*query)[fieldDouble.first].set_doublevalue(fieldDouble.second);
  }

  query->index(); // FIXME (titus) just index if not indexed or so...
  // TODO(tcies) return NULL if empty result
  return query;
}

bool TableInterface::updateQuery(const Hash& id,
                                 const TableInsertQuery& query){
  // TODO(tcies) all concurrency handling, owner locking, etc... comes here
  Poco::Data::Statement stat(*session_);
  stat << "UPDATE " << name() << " SET ";
  // TODO(discuss) I don't like the redundancy of this code, but it's always
  // slightly different... not sure if any abstraction would be worth?
  for (int i=0; i<query.fieldqueries_size(); ++i){
    if (i>0){
      stat << ", ";
    }
    const proto::TableField& field = query.fieldqueries(i);
    stat << field.nametype().name() << "=";
    switch(field.nametype().type()){
      case (proto::TableFieldDescriptor_Type_BLOB):{
        // TODO(tcies) not using poco blobs as in insert, as they don't fix the
        // issue of sometimes problematic insertions anyways
        stat << ":" << field.nametype().name() << " ",
            Poco::Data::use(field.blobvalue());
        break;
      }
      case (proto::TableFieldDescriptor_Type_DOUBLE):{
        stat << ":" << field.nametype().name() << " ",
            Poco::Data::use(field.doublevalue());
        break;
      }
      case (proto::TableFieldDescriptor_Type_STRING): // Fallthrough intended
      case (proto::TableFieldDescriptor_Type_HASH128):{
        stat << ":" << field.nametype().name() << " ",
            Poco::Data::use(field.stringvalue());
        break;
      }
      default:{
        LOG(FATAL) << "Type of field supplied to update query unknown";
      }
    }
  }
  stat << "WHERE ID LIKE :id", Poco::Data::use(id.getString());

  stat.execute();
  return stat.done();
}

}
