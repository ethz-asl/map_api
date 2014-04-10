/*
 * write-only-table-interface.cc
 *
 *  Created on: Apr 4, 2014
 *      Author: titus
 */

#include <map-api/cru-table-interface.h>

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
#include "map-api/transaction.h"
#include "core.pb.h"

DEFINE_string(ipPort, "127.0.0.1:5050", "Define node ip and port");

namespace map_api {

const Hash& CRTableInterface::getOwner() const{
  return owner_;
}

bool CRTableInterface::addField(const std::string& name,
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

bool CRTableInterface::setup(const std::string& name){
  // TODO(tcies) Test before initialized or RAII
  // TODO(tcies) check whether string safe for SQL, e.g. no hyphens
  set_name(name);
  // Define table fields
  // enforced fields id (hash) and owner
  addField<Hash>("ID");
  addField<Hash>("owner");
  // transaction-enforced fields
  std::shared_ptr<std::vector<proto::TableFieldDescriptor> >
  transactionFields(Transaction::requiredTableFields());
  for (const proto::TableFieldDescriptor& descriptor :
      *transactionFields){
    addField(descriptor.name(), descriptor.type());
  }
  // user-defined fields
  define();

  // start up core if not running yet TODO(tcies) do this in the core
  if (!MapApiCore::getInstance().isInitialized()) {
    MapApiCore::getInstance().init(FLAGS_ipPort);
  }

  // choose owner ID TODO(tcies) this is temporary
  // TODO(tcies) make shareable across table interfaces
  owner_ = Hash::randomHash();
  LOG(INFO) << "Table interface with owner " << owner_.getString();

  // connect to database & create table
  // TODO(tcies) register in master table
  session_ = MapApiCore::getInstance().getSession();
  createQuery();

  // Sync with cluster TODO(tcies)
  // sync();
  return true;
}

std::shared_ptr<Revision> CRTableInterface::getTemplate() const{
  std::shared_ptr<Revision> ret =
      std::shared_ptr<Revision>(
          new Revision);
  // add own name
  ret->set_table(name());
  // add editable fields
  for (int i=0; i<this->fields_size(); ++i){
    *(ret->add_fieldqueries()->mutable_nametype()) = this->fields(i);
  }
  ret->index();
  return ret;
}

bool CRTableInterface::createQuery(){
  Poco::Data::Statement stat(*session_);
  stat << "CREATE TABLE IF NOT EXISTS " << name() << " (";
  // parse fields from descriptor as database fields
  for (int i=0; i<this->fields_size(); ++i){
    const proto::TableFieldDescriptor &fieldDescriptor = this->fields(i);
    TableField field;
    // The following is specified in protobuf but not available.
    // We are using an outdated version of protobuf.
    // Consider upgrading once overwhelmingly necessary.
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

// TODO(tcies) pass by reference to shared pointer
map_api::Hash CRTableInterface::insertQuery(Revision& query){
  // set ID (TODO(tcies): set owner as well)
  map_api::Hash idHash(query.SerializeAsString());
  query["ID"].set(idHash);
  query["owner"].set(owner_);

  Transaction transaction(owner_);
  // TODO(tcies) all the checks...
  transaction.begin();
  transaction.addInsertQuery(
      Transaction::SharedRevisionPointer(new Revision(query)));
  transaction.commit();
  // TODO(tcies) check if aborted
  return idHash;
}

std::shared_ptr<Revision> CRTableInterface::getRow(
    const map_api::Hash &id) const{
  std::shared_ptr<Revision> query = getTemplate();
  Poco::Data::Statement stat(*session_);
  stat << "SELECT ";

  // because protobuf won't supply mutable pointers to numeric values, we can't
  // pass them as reference to the into() binding of Poco::Data; they have to
  // be assigned a posteriori - this is done through this map
  std::map<std::string, double> doublePostApply;
  std::map<std::string, int32_t> intPostApply;
  std::map<std::string, int64_t> longPostApply;
  std::map<std::string, Poco::Data::BLOB> blobPostApply;

  for (int i=0; i<query->fieldqueries_size(); ++i){
    if (i>0){
      stat << ", ";
    }
    proto::TableField& field = *query->mutable_fieldqueries(i);
    stat << field.nametype().name();
    // TODO(simon) do you see a reasonable way to move this to TableField?
    switch(field.nametype().type()){
      case (proto::TableFieldDescriptor_Type_BLOB):{
        stat, Poco::Data::into(blobPostApply[field.nametype().name()]);
        break;
      }
      case (proto::TableFieldDescriptor_Type_DOUBLE):{
        stat, Poco::Data::into(doublePostApply[field.nametype().name()]);
        break;
      }
      case (proto::TableFieldDescriptor_Type_INT32):{
        stat, Poco::Data::into(intPostApply[field.nametype().name()]);
        break;
      }
      case (proto::TableFieldDescriptor_Type_INT64):{
        stat, Poco::Data::into(longPostApply[field.nametype().name()]);
        break;
      }
      case (proto::TableFieldDescriptor_Type_STRING): // Fallthrough intended
      case (proto::TableFieldDescriptor_Type_HASH128):{
        // default string value allows us to see whether a query failed by
        // looking at the ID
        stat, Poco::Data::into(*field.mutable_stringvalue(),
                               std::string(""));
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
  } catch (std::exception& e){
    LOG(ERROR) << "Statement failed transaction: " << stat.toString();
    return std::shared_ptr<Revision>();
  }

  // indication of empty result
  if ((*query)["ID"].get<Hash>().getString() == ""){
    return std::shared_ptr<Revision>();
  }

  // write values that couldn't be written directly
  for (std::pair<std::string, double> fieldDouble : doublePostApply){
    (*query)[fieldDouble.first].set_doublevalue(fieldDouble.second);
  }
  for (std::pair<std::string, int32_t> fieldInt : intPostApply){
    (*query)[fieldInt.first].set_intvalue(fieldInt.second);
  }
  for (std::pair<std::string, int64_t> fieldLong : longPostApply){
    (*query)[fieldLong.first].set_longvalue(fieldLong.second);
  }
  for (const std::pair<std::string, Poco::Data::BLOB>& fieldBlob :
      blobPostApply){
    (*query)[fieldBlob.first].set_blobvalue(fieldBlob.second.rawContent(),
                                            fieldBlob.second.size());
  }

  query->index(); // FIXME (titus) just index if not indexed or so...
  // TODO(tcies) return NULL if empty result
  return query;
}

} /* namespace map_api */
