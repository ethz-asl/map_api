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

bool CRUTableInterface::setup(const std::string &name){
  // TODO(tcies) outsource tasks common with write-only table interface
  // TODO(tcies) Test before initialized or RAII
  // TODO(tcies) check whether string safe for SQL, e.g. no hyphens
  set_name(name);
  // Define table fields
  // enforced fields id (hash) and owner
  addField("ID",proto::TableFieldDescriptor_Type_HASH128);
  addField("owner",proto::TableFieldDescriptor_Type_HASH128);
  // transaction-enforced fields TODO(tcies) later
  // std::shared_ptr<std::vector<proto::TableFieldDescriptor> >
  // transactionFields(Transaction::requiredTableFields());
  // for (const proto::TableFieldDescriptor& descriptor :
  //     *transactionFields){
  //   addField(descriptor.name(), descriptor.type());
  // }
  // user-defined fields
  define();

  // start up core if not running yet TODO(tcies) do this in the core
  if (!MapApiCore::getInstance().isInitialized()) {
    MapApiCore::getInstance().init(FLAGS_ipPort);
  }

  // choose owner ID TODO(tcies) this is temporary
  // TODO(tcies) make shareable across table interfaces
  owner_ = Hash::randomHash();
  VLOG(3) << "Table interface with owner " << owner_.getString();

  // connect to database & create table
  // TODO(tcies) register in master table
  session_ = MapApiCore::getInstance().getSession();
  createQuery();

  // Sync with cluster TODO(tcies)
  // sync();
  return true;
}


bool CRUTableInterface::rawUpdateQuery(const Hash& id,
                                       const Hash& nextRevision){
  // TODO(tcies) adapt to transaction-centricity

  /*

  // Bag for blobs that need to stay in scope until statement is executed
  std::vector<std::shared_ptr<Poco::Data::BLOB> > blobBag;

  Poco::Data::Statement stat(*session_);
  stat << "UPDATE " << name() << " SET ";
  for (int i=0; i<query.fieldqueries_size(); ++i){
    if (i>0){
      stat << ", ";
    }
    const proto::TableField& field = query.fieldqueries(i);
    stat << field.nametype().name() << "=";
    // TODO(tcies) prettify, uncast
    blobBag.push_back(query.insertPlaceHolder(i, stat));
  }
  stat << "WHERE ID LIKE :id", Poco::Data::use(id.getString());

  stat.execute();
  return stat.done();

   */
  return false;
}

}
