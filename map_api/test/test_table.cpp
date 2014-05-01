/*
 * test_table.cpp
 *
 *  Created on: Apr 14, 2014
 *      Author: titus
 */

/**
 * A test table revealing some more internals than a typical table, such as
 * template, database session and cleanup.
 */
#include <map-api/cru-table-interface.h>
#include <glog/logging.h>

class TestTable : public map_api::CRUTableInterface {
 public:
  TestTable(map_api::Id owner) : map_api::CRUTableInterface(owner) {}
  ~TestTable() {}
  virtual bool init(){
    setup("test_table");
    return true;
  }
  std::shared_ptr<map_api::Revision> templateForward() const{
    return getTemplate();
  }
  std::shared_ptr<Poco::Data::Session> sessionForward(){
    return std::shared_ptr<Poco::Data::Session>(session_);
  }
  void cleanup(){
    *(sessionForward()) << "DROP TABLE IF EXISTS " << name(),
        Poco::Data::now;
    LOG(INFO) << "Table " << name() << " dropped";
  }
 protected:
  virtual bool define(){
    return true;
  }
};
