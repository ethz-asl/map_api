#include "map-api/cru-table-interface.h"

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

CRUTableInterface::~CRUTableInterface() {}

bool CRUTableInterface::init() {
  const std::string name(tableName());
  // TODO(tcies) unite with CRTableInterface::init
  // verify name is SQL friendly: For now very tight constraints:
  for (const char& character : name) {
    CHECK((character >= 'A' && character <= 'Z') ||
          (character >= 'a' && character <= 'z') ||
          (character == '_')) << "Desired table name \"" << name <<
              "\" ill-suited for SQL database";
  }
  // Define fields of content (that will be outsourced to history
  {
    // user will call addField in define, which has been overriden here to
    // define the structure that is exported to the history
    define();
  }
  // Define fields of the actual CRU table: Reference to latest history item.
  {
    addCRUField<Id>("ID");
    // addCRUField<Id>("owner"); TODO(tcies) later
    addCRUField<Id>("latest_revision");
  }
  // Set table name TODO(tcies) string SQL-ready, e.g. no hyphens?
  set_name(name);
  // connect to database & create table
  session_ = MapApiCore::getInstance().getSession();
  if (!sync()){
    return false;
  }
  if (!createQuery()){
    LOG(ERROR) << "Failed to create table";
  }
  // initialize history table
  history_ = std::unique_ptr<History>(new History(name));
  if (!history_->init()){
    LOG(ERROR) << "Failed to initialize history";
    return false;
  }
  initialized_ = true;
  return true;
}

std::shared_ptr<Revision> CRUTableInterface::getTemplate() const{
  std::shared_ptr<Revision> ret(new Revision);
  // add own name
  ret->set_table(name());
  // add editable fields
  for (int i = 0; i < descriptor_.fields_size(); ++i){
    ret->addField(descriptor_.fields(i));
  }
  return ret;
}

std::shared_ptr<Revision> CRUTableInterface::getCRUTemplate() const{
  return CRTableInterface::getTemplate();
}

bool CRUTableInterface::addField(const std::string& name,
                                 proto::TableFieldDescriptor_Type type){
  // same code as in CR table, except that setting fields on descriptor member,
  // not the table interface itself
  // make sure the field has not been defined yet
  for (int i = 0; i < descriptor_.fields_size(); ++i){
    // TODO (tcies) error handling
    CHECK_NE(descriptor_.fields(i).name().compare(name), 0) <<
        "In descriptor of table " << this->name() << ": Field " <<
        name << " defined twice!";
  }
  proto::TableFieldDescriptor *field = descriptor_.add_fields();
  field->set_name(name);
  field->set_type(type);
  return true;
}


bool CRUTableInterface::rawUpdateQuery(const Id& id,
                                       const Id& nextRevision) const{
  Poco::Data::Statement stat(*session_);
  // needs to persist until the statement is executed
  std::string idString = id.hexString();
  std::string nextRevisionString = nextRevision.hexString();
  stat << "UPDATE " << name() <<
      " SET latest_revision = ? ", Poco::Data::use(nextRevisionString);
  stat << "WHERE ID LIKE :id", Poco::Data::use(idString);
  try {
    stat.execute();
  } catch (const std::exception& e) {
    LOG(ERROR) << "Update failed with exception " << e.what();
    return false;
  }
  return stat.done();
}

bool CRUTableInterface::rawLatestUpdate(const Id& id, Time* time) const{
  std::shared_ptr<Revision> row = rawGetRow(id);
  ItemDebugInfo itemInfo(name(), id);
  if (!row){
    LOG(ERROR) << itemInfo << "Failed to retrieve row";
    return false;
  }
  Id latestInHistoryId;
  if (!row->get("latest_revision", &latestInHistoryId)){
    LOG(ERROR) << itemInfo << "Does not contain 'latest_revision'";
    return false;
  }
  std::shared_ptr<Revision> latestInHistory(
      history_->rawGetRow(latestInHistoryId));
  if (!latestInHistory){
    LOG(ERROR) << itemInfo << "Failed to get latest revision in history";
    return false;
  }
  if (!latestInHistory->get("time", time)){
    LOG(ERROR) << itemInfo << "Latest revision does not contain 'time'";
    return false;
  }
  return true;
}

}
