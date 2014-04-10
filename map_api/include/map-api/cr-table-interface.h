/*
 * write-only-table-interface.h
 *
 *  Created on: Apr 4, 2014
 *      Author: titus
 */

#ifndef WRITE_ONLY_TABLE_INTERFACE_H_
#define WRITE_ONLY_TABLE_INTERFACE_H_

#include <vector>
#include <memory>
#include <map>

#include <Poco/Data/Common.h>
#include <gflags/gflags.h>

#include "map-api/hash.h"
#include "map-api/revision.h"
#include "map-api/cr-table-interface.h"
#include "core.pb.h"

namespace map_api {

class CRTableInterface : public proto::TableDescriptor {
 public:
  /**
   * Init routine, must be implemented by derived class, defines table name.
   * TODO(tcies) enforce? isInitialized?
   */
  virtual bool init() = 0;

  /**
   * TODO(tcies) might drop notion of owner for write-only tables - it's
   * probably not really absolutely required, unlike in updatable tables, where
   * it's needed to lock.
   */
  const Hash& getOwner() const;

 protected:
  /**
   * Setup: Load table definition and match with table definition in
   * cluster.
   */
  virtual bool setup(const std::string& name);
  /**
   * Function to be implemented by derivations: Define table by repeated
   * calls to addField()
   */
  virtual bool define() = 0;
  /**
   * Returns a table row template
   */
  std::shared_ptr<Revision> getTemplate() const;
  /**
   * Function to be called at definition:  Adds field to table
   */
  template<typename Type>
  bool addField(const std::string& name);
  /**                                                                       CCCC
   *                                                                       C
   * Commits an insert query                                               C
   *                                                                       C
   *                                                                        CCCC
   */
  Hash insertQuery(Revision& query);
  /**                                                                      RRRR
   *                                                                       R   R
   * Fetches row by ID and returns it as filled TableInsertQuery           RRRR
   *                                                                       R  R
   *                                                                       R   R
   */
  std::shared_ptr<Revision> getRow(const Hash& id) const;
  /**
   * Shared pointer to database session TODO(tcies) can this be set private
   * yet accessed from a test table?
   */
  std::shared_ptr<Poco::Data::Session> session_;

 private:
  friend class CRUTableInterface;
  /**
   * Synchronize with cluster: Check if table already present in cluster
   * metatable, add user to distributed table
   */
  bool sync();
  /**
   * Parse and execute SQL query necessary to create the database
   */
  bool createQuery();

  bool addField(const std::string& name, proto::TableFieldDescriptor_Type type);
  /**
   * On one hand, the cache is used to test concurrency concepts with a single
   * process. On the other hand, it can be used for access speedup later on.
   */
  std::map<Hash, Revision> cache_;
  Hash owner_;

};

} /* namespace map_api */

#include "map-api/cr-table-interface-inl.h"

#endif /* WRITE_ONLY_TABLE_INTERFACE_H_ */
