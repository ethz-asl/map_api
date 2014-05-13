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
#include "map-api/transaction.h"
#include "core.pb.h"

namespace map_api {

CRTableInterface::CRTableInterface(const Id& owner) : owner_(owner),
    initialized_(false) {}

CRTableInterface::~CRTableInterface() {}

const Id& CRTableInterface::getOwner() const{
  return owner_;
}

bool CRTableInterface::isInitialized() const{
  return initialized_;
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
  addField<Id>("ID");
  addField<Id>("owner");
  // transaction-enforced fields TODO(tcies) later
  // std::shared_ptr<std::vector<proto::TableFieldDescriptor> >
  // transactionFields(Transaction::requiredTableFields());
  // for (const proto::TableFieldDescriptor& descriptor :
  //     *transactionFields){
  //   addField(descriptor.name(), descriptor.type());
  // }
  // user-defined fields
  define();

  // connect to database & create table
  // TODO(tcies) register in master table
  session_ = MapApiCore::getInstance().getSession();
  createQuery();

  // Sync with cluster TODO(tcies)
  // sync();
  initialized_ = true;
  return true;
}

std::shared_ptr<Revision> CRTableInterface::getTemplate() const{
  std::shared_ptr<Revision> ret =
      std::shared_ptr<Revision>(
          new Revision);
  // add own name
  ret->set_table(name());
  // add editable fields
  for (int i = 0; i < fields_size(); ++i){
    ret->addField(fields(i));
  }
  return ret;
}

bool CRTableInterface::createQuery(){
  Poco::Data::Statement stat(*session_);
  stat << "CREATE TABLE IF NOT EXISTS " << name() << " (";
  // parse fields from descriptor as database fields
  for (int i=0; i<this->fields_size(); ++i){
    const proto::TableFieldDescriptor &fieldDescriptor = this->fields(i);
    proto::TableField field;
    // The following is specified in protobuf but not available.
    // We are using an outdated version of protobuf.
    // Consider upgrading once overwhelmingly necessary.
    // field.set_allocated_nametype(&fieldDescriptor);
    *field.mutable_nametype() = fieldDescriptor;
    if (i != 0){
      stat << ", ";
    }
    stat << fieldDescriptor.name() << " ";
    switch (fieldDescriptor.type()){
      case (proto::TableFieldDescriptor_Type_BLOB): stat << "BLOB"; break;
      case (proto::TableFieldDescriptor_Type_DOUBLE): stat << "REAL"; break;
      case (proto::TableFieldDescriptor_Type_HASH128): stat << "TEXT"; break;
      case (proto::TableFieldDescriptor_Type_INT32): stat << "INTEGER"; break;
      case (proto::TableFieldDescriptor_Type_INT64): stat << "INTEGER"; break;
      case (proto::TableFieldDescriptor_Type_STRING): stat << "TEXT"; break;
      default:
        LOG(FATAL) << "Field type not handled";
    }
    if (fieldDescriptor.name().compare("ID") == 0){
      stat << " PRIMARY KEY";
    }
  }
  stat << ");";

  try {
    stat.execute();
  } catch(const std::exception &e){
    LOG(FATAL) << "Create failed with exception " << e.what();
  }

  return true;
}

bool CRTableInterface::rawInsertQuery(const Revision& query) const{
  // TODO(tcies) verify schema

  // Bag for blobs that need to stay in scope until statement is executed
  std::vector<std::shared_ptr<Poco::Data::BLOB> > placeholderBlobs;

  // assemble SQLite statement
  Poco::Data::Statement stat(*session_);
  // NB: sqlite placeholders work only for column values
  stat << "INSERT INTO " << name() << " ";

  stat << "(";
  for (int i = 0; i < query.fieldqueries_size(); ++i){
    if (i > 0){
      stat << ", ";
    }
    stat << query.fieldqueries(i).nametype().name();
  }
  stat << ") VALUES ( ";
  for (int i = 0; i < query.fieldqueries_size(); ++i){
    if (i > 0){
      stat << " , ";
    }
    placeholderBlobs.push_back(query.insertPlaceHolder(i,stat));
  }
  stat << " ); ";

  try {
    stat.execute();
  } catch(const std::exception &e){
    LOG(ERROR) << "Insert failed with exception " << e.what();
    return false;
  }

  return true;
}

std::shared_ptr<Revision> CRTableInterface::rawGetRow(
    const Id &id) const{
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
  std::map<std::string, std::string> stringPostApply;
  std::map<std::string, std::string> hashPostApply;

  for (int i=0; i<query->fieldqueries_size(); ++i){
    if (i>0){
      stat << ", ";
    }
    const proto::TableField& field = query->fieldqueries(i);
    stat << field.nametype().name();
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
      case (proto::TableFieldDescriptor_Type_STRING):{
        stat, Poco::Data::into(stringPostApply[field.nametype().name()]);
        break;
      }
      case (proto::TableFieldDescriptor_Type_HASH128):{
        // default string value allows us to see whether a query failed by
        // looking at the ID
        stat, Poco::Data::into(hashPostApply[field.nametype().name()],
                               std::string(""));
        break;
      }
      default:{
        LOG(FATAL) << "Type of field supplied to select query unknown" <<
            std::endl;
      }
    }
  }

  // ID string must remain in scope until the statement is executed, so we need
  // an explicit copy
  std::string idString = id.hexString();
  stat << " FROM " << name() << " WHERE ID LIKE :id",
      Poco::Data::use(idString);

  try{
    stat.execute();
  } catch (const std::exception& e){
    LOG(ERROR) << "Statement failed transaction: " << stat.toString();
    return std::shared_ptr<Revision>();
  }

  // indication of empty result
  if (hashPostApply["ID"] == ""){
    // sometimes, queries fail intentionally, such as when checking for conflict
    // when inserting
    VLOG(3) << "Database query for " << id.hexString() << " in table " <<
        name() << " returned empty result, query was " << stat.toString();
    return std::shared_ptr<Revision>();
  }

  // write values
  for (const std::pair<std::string, double>& fieldDouble : doublePostApply){
    query->set(fieldDouble.first, fieldDouble.second);
  }
  for (const std::pair<std::string, int32_t>& fieldInt : intPostApply){
    query->set(fieldInt.first, fieldInt.second);
  }
  for (const std::pair<std::string, int64_t>& fieldLong : longPostApply){
    query->set(fieldLong.first, fieldLong.second);
  }
  for (const std::pair<std::string, Poco::Data::BLOB>& fieldBlob :
      blobPostApply){
    query->set(fieldBlob.first, fieldBlob.second);
  }
  for (const std::pair<std::string, std::string>& fieldString :
      stringPostApply){
    query->set(fieldString.first, fieldString.second);
  }
  for (const std::pair<std::string, std::string>& fieldHash :
      hashPostApply){
    Id value;
    if (!value.fromHexString(fieldHash.second)){
      LOG(FATAL) << "Hex string " << fieldHash.second <<
          "in table can't parse to Hash ID";
    }
    query->set(fieldHash.first, value);
  }
  return query;
}

// although this is very similar to rawGetRow(), I don't see how to share the
// features without loss of performance TODO(discuss)
bool CRTableInterface::rawDump(std::vector<std::shared_ptr<Revision> >* dest)
const{
  CHECK_NOTNULL(dest);
  std::shared_ptr<Revision> query = getTemplate();
  Poco::Data::Statement stat(*session_);
  stat << "SELECT ";

  // because protobuf won't supply mutable pointers to numeric values, we can't
  // pass them as reference to the into() binding of Poco::Data; they have to
  // be assigned a posteriori - this is done through this map
  std::map<std::string, std::vector<double> > doublePostApply;
  std::map<std::string, std::vector<int32_t> > intPostApply;
  std::map<std::string, std::vector<int64_t> > longPostApply;
  std::map<std::string, std::vector<Poco::Data::BLOB> > blobPostApply;
  std::map<std::string, std::vector<std::string> > stringPostApply;
  std::map<std::string, std::vector<std::string> > hashPostApply;

  for (int i = 0; i < query->fieldqueries_size(); ++i) {
    if (i>0) {
      stat << ", ";
    }
    const proto::TableField& field = query->fieldqueries(i);
    stat << field.nametype().name();
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
      case (proto::TableFieldDescriptor_Type_STRING):{
        stat, Poco::Data::into(stringPostApply[field.nametype().name()]);
        break;
      }
      case (proto::TableFieldDescriptor_Type_HASH128):{
        // default string value allows us to see whether a query failed by
        // looking at the ID
        stat, Poco::Data::into(hashPostApply[field.nametype().name()]);
        break;
      }
      default:{
        LOG(FATAL) << "Type of field supplied to select query unknown";
      }
    }
  }

  stat << " FROM " << name();

  try{
    stat.execute();
  } catch (const std::exception& e){
    LOG(FATAL) << "Failed fetching all items from table: " << stat.toString()
        << " with exception " << e.what();
  }

  // reserve output
  CHECK(hashPostApply.find("ID") != hashPostApply.end());
  dest->resize(hashPostApply["ID"].size());

  // write values
  for (size_t i = 0; i < dest->size(); ++i) {
    (*dest)[i] = getTemplate();
    for (const std::pair<std::string, std::vector<double> >& fieldDouble :
        doublePostApply){
      (*dest)[i]->set(fieldDouble.first, fieldDouble.second[i]);
    }
    for (const std::pair<std::string, std::vector<int32_t> >& fieldInt :
        intPostApply){
      (*dest)[i]->set(fieldInt.first, fieldInt.second[i]);
    }
    for (const std::pair<std::string, std::vector<int64_t> >& fieldLong :
        longPostApply){
      (*dest)[i]->set(fieldLong.first, fieldLong.second[i]);
    }
    for (const std::pair<std::string, std::vector<Poco::Data::BLOB> >&
        fieldBlob : blobPostApply){
      (*dest)[i]->set(fieldBlob.first, fieldBlob.second[i]);
    }
    for (const std::pair<std::string, std::vector<std::string> >& fieldString :
        stringPostApply){
      (*dest)[i]->set(fieldString.first, fieldString.second[i]);
    }
    for (const std::pair<std::string, std::vector<std::string> >& fieldHash :
        hashPostApply){
      Id value;
      CHECK(value.fromHexString(fieldHash.second[i]));
      (*dest)[i]->set(fieldHash.first, value);
    }
  }
  return true;
}

std::ostream& operator<< (std::ostream& stream,
                          const CRTableInterface::ItemDebugInfo& info){
  return stream << "For table " << info.table << ", item " << info.id << ": ";
}

} /* namespace map_api */
