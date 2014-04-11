/*
 * TableInterface.cpp
 *
 *  Created on: Mar 6, 2014
 *      Author: titus
 */

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

DECLARE_string(ipPort);

namespace map_api {

// TODO(tcies) oh no, can't initialize history!! unique_ptr or move owner
// declaration to init() function
CRUTableInterface::CRUTableInterface(Hash owner) : CRTableInterface(owner),
    history_("FIXME!!!", owner) {}

bool CRUTableInterface::setup(const std::string &name){
  // First part: Define fields of content (that will be outsourced to history
  {
    // user will call addField in define, which has been overriden here to
    // define the structure that is exported to the history
    define();
  }

  // Second part: Define fields of the actual CRU table: Householding of
  // references to history items. Very simple:
  {
    addCRUField<Hash>("ID");
    addCRUField<Hash>("owner");
    addCRUField<Hash>("latest_revision");
  }

  // Third part: Various other setup operations
  {
    // Fire up core TODO(tcies) core auto-start?
    if (!MapApiCore::getInstance().isInitialized()) {
      MapApiCore::getInstance().init(FLAGS_ipPort);
    }
    // Set table name TODO(tcies) string SQL-ready, e.g. no hyphens?
    set_name(name);

    // connect to database & create table
    // TODO(tcies) register in master table
    session_ = MapApiCore::getInstance().getSession();
    createQuery();
  }
  return true;
}

bool CRUTableInterface::addField(const std::string& name,
                                 proto::TableFieldDescriptor_Type type){
  // same code as in CR table, except that setting fields on descriptor member,
  // not the table interface itself
  // make sure the field has not been defined yet
  for (int i=0; i<descriptor_.fields_size(); ++i){
    if (descriptor_.fields(i).name().compare(name) == 0){
      LOG(FATAL) << "In descriptor of table " << this->name() << ": Field " <<
          name << " defined twice!" << std::endl;
    }
  }
  // otherwise, proceed with adding field
  proto::TableFieldDescriptor *field = descriptor_.add_fields();
  field->set_name(name);
  field->set_type(type);
  return true;
}


bool CRUTableInterface::rawUpdateQuery(const Hash& id,
                                       const Hash& nextRevision){
  Poco::Data::Statement stat(*session_);
  stat << "UPDATE " << name() <<
      " SET latest_revision = ? ", Poco::Data::use(nextRevision.getString());
  stat << "WHERE ID LIKE :id", Poco::Data::use(id.getString());
  stat.execute();
  return stat.done();
}

}
