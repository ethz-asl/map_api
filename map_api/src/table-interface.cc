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
  ses_ = MapApiCore::getInstance().getSession();
  createQuery();

  // Sync with cluster TODO(tcies)
  // sync();
  LOG(INFO) << "Ok, " << name << " initialized!" << std::endl;
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
  Poco::Data::Statement stat(*ses_);
  stat << "CREATE TABLE IF NOT EXISTS " << name() << " (";

  // parse fields from descriptor as database fields
  for (int i=0; i<this->fields_size(); ++i){
    const proto::TableFieldDescriptor &field = this->fields(i);
    if (i != 0){
      stat << ", ";
    }
    stat << field.name();
    switch(field.type()){
      case (proto::TableFieldDescriptor_Type_BLOB):{
        stat << " BLOB";
        break;
      }
      // TODO(discuss) this is rather SQLite specific...
      case (proto::TableFieldDescriptor_Type_STRING): // Fallthrough intended
      case (proto::TableFieldDescriptor_Type_HASH128):{
        stat << " TEXT";
        // make ID the index of the table
        if (field.name().compare("ID") == 0){
          stat << " PRIMARY KEY";
        }
        break;
      }
      default:
        LOG(FATAL) << "Type of field supplied to create query unknown" <<
        std::endl;
    }
  }

  stat << ");";

  LOG(INFO) << stat.toString();
  stat.execute();

  return true;
}

map_api::Hash TableInterface::insertQuery(TableInsertQuery& query){
  // set ID (TODO(tcies): set owner as well)
  map_api::Hash idHash(query.SerializeAsString());
  query["ID"]->set_stringvalue(idHash.getString());

  // assemble SQLite statement
  Poco::Data::Statement stat(*ses_);
  // NB: sqlite placeholders work only for column values
  stat << "INSERT INTO " << name() << " ";

  std::vector<std::string> fields;
  std::vector<Poco::Data::BLOB> values;

  // collect fields
  for (int i=0; i<query.fieldqueries_size(); ++i){
    const proto::TableField& field = query.fieldqueries(i);
    fields.push_back(field.nametype().name());
    switch(field.nametype().type()){
      case (proto::TableFieldDescriptor_Type_BLOB):{
        values.push_back(Poco::Data::BLOB(field.blobvalue()));
        break;
      }
      case (proto::TableFieldDescriptor_Type_DOUBLE):{
        std::stringstream ss;
        ss << field.doublevalue();
        values.push_back(Poco::Data::BLOB(ss.str()));
        break;
      }
      case (proto::TableFieldDescriptor_Type_STRING): // Fallthrough intended
      case (proto::TableFieldDescriptor_Type_HASH128):{
        values.push_back(Poco::Data::BLOB(field.stringvalue()));
        break;
      }
      default:{
        LOG(FATAL) << "Type of field supplied to insert query unknown" <<
            std::endl;
        return map_api::Hash();
      }
    }
  }
  if (fields.size() != values.size()){
    LOG(FATAL) << "Field declaration and value size don't match" <<
        std::endl;
  }

  // declare fields
  stat << "(";
  for (unsigned int i=0; i<fields.size(); ++i){
    if (i>0){
      stat << ", ";
    }
    // NB: sqlite placeholders work only for column values
    stat << fields[i];
  }
  stat << ")";

  stat << " VALUES ";

  stat << "(";
  for (unsigned int i=0; i<values.size(); ++i){
    if (i>0){
      stat << ", ";
    }
    stat << ":blob" << i << " ", Poco::Data::use(values[i]);
  }
  stat << ");";

  stat.execute();

  // TODO (tcies) trigger subscribers
  return idHash;
}

std::shared_ptr<TableInsertQuery> TableInterface::getRow(
    const map_api::Hash &id) const{
  std::shared_ptr<TableInsertQuery> query = getTemplate();
  Poco::Data::Statement stat(*ses_);
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

  stat.execute();

  // write values that couldn't be written directly
  for (std::pair<std::string, double> fieldDouble : doublePostApply){
    (*query)[fieldDouble.first]->set_doublevalue(fieldDouble.second);
  }

  query->index(); // FIXME (titus) just index if not indexed or so...
  // TODO(tcies) return NULL if empty result
  return query;
}

bool TableInterface::updateQuery(const Hash& id,
                                 const TableInsertQuery& query){
  // TODO(tcies) all concurrency handling, owner locking, etc... comes here
  Poco::Data::Statement stat(*ses_);
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
        LOG(FATAL) << "Type of field supplied to update query unknown" <<
            std::endl;
      }
    }
  }
  stat << "WHERE ID=:id", Poco::Data::use(id.getString());

  stat.execute();
  return true;
}

}
