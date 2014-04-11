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
   * Constructor does not throw, just sets owner
   */
  CRTableInterface(const Hash& owner);
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
  bool setup(const std::string& name);
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
   * Function to be called at definition:  Adds field to table. This only calls
   * the other addField function with the proper enum, see implementation
   * header.
   */
  template<typename Type>
  bool addField(const std::string& name);
  bool addField(const std::string& name,
                        proto::TableFieldDescriptor_Type type);
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

  Hash owner_;

  /**
   * The following functions are to be used by transactions only. They pose a
   * very crude access straight to the database, without synchronization
   * and conflict checking - that is assumed to be done by the transaction.
   * History is another example at it is managed by the transaction.
   */
  friend class Transaction;
  friend class History;
  /**                                                                       CCCC
   *                                                                       C
   * Commits an insert query. ID has to be defined in the query, this is   C
   * responsability of the transaction.                                    C
   *                                                                        CCCC
   */
  bool rawInsertQuery(Revision& query);
  /**                                                                      RRRR
   *                                                                       R   R
   * Fetches row by ID and returns it as revision                          RRRR
   *                                                                       R  R
   *                                                                       R   R
   */
  std::shared_ptr<Revision> rawGetRow(const Hash& id) const;

};

} /* namespace map_api */

#include "map-api/cr-table-interface-inl.h"

#endif /* WRITE_ONLY_TABLE_INTERFACE_H_ */
