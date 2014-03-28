#ifndef TABLE_INTERFACE_H
#define TABLE_INTERFACE_H

#include <vector>
#include <memory>

#include <Poco/Data/Common.h>
#include <gflags/gflags.h>

#include "map-api/hash.h"
#include "map-api/table-insert-query.h"
#include "core.pb.h"

namespace map_api {

/**
 * Provides interface to map api tables.
 */
class TableInterface : public proto::TableDescriptor {
 public:
  /**
   * Init routine, must be implemented by derived class, defines table name.
   */
  virtual bool init() = 0;

 protected:
  /**
   * Setup: Load table definition and match with table definition in
   * cluster.
   */
  bool setup(std::string name);
  /**
   * Function to be implemented by derivations: Define table by populating
   * TableDescriptor protobuf message.
   */
  virtual bool define();
  /**
   * Returns a table row template
   */
  std::shared_ptr<TableInsertQuery> getTemplate() const;
  /**
   * Function to be called at definition:  Adds field to table
   */
  bool addField(std::string name, proto::TableFieldDescriptor_Type type);
  /**                                                                       CCCC
   *                                                                       C
   * Commits an insert query                                               C
   *                                                                       C
   *                                                                        CCCC
   */
  Hash insertQuery(TableInsertQuery& query);
  /**                                                                      RRRR
   *                                                                       R   R
   * Fetches row by ID and returns it as filled TableInsertQuery           RRRR
   *                                                                       R  R
   *                                                                       R   R
   */
  std::shared_ptr<TableInsertQuery> getRow(const Hash& id) const;
  /**                                                                      U   U
   *                                                                       U   U
   * Takes hash ID and TableInsertQuery as argument and updates the row of U   U
   * the given ID with the query                                           U   U
   *                                                                        UUU
   */
  bool updateQuery(const Hash& id, const TableInsertQuery& query);


 private:
  /**
   * Synchronize with cluster: Check if table already present in cluster
   * metatable, add user to distributed table
   */
  bool sync();
  /**
   * Parse and execute SQL query necessary to create the database
   */
  bool createQuery();

  /**
   * Shared pointer to database session
   */
  std::shared_ptr<Poco::Data::Session> ses_;
};

}

#endif  // TABLE_INTERFACE_H
