#ifndef TABLE_INTERFACE_H
#define TABLE_INTERFACE_H

#include <vector>
#include <memory>
#include <map>

#include <Poco/Data/Common.h>
#include <gflags/gflags.h>

#include "map-api/hash.h"
#include "map-api/revision.h"
#include "map-api/write-only-table-interface.h"
#include "core.pb.h"

namespace map_api {

/**
 * Provides interface to map api tables.
 */
class TableInterface : public WriteOnlyTableInterface{
 public:
  virtual bool init() = 0;
 protected:
  /**
   * Setup: Load table definition and match with table definition in
   * cluster.
   */
  virtual bool setup(std::string name);
  virtual bool define() = 0;
  /**                                                                      U   U
   *                                                                       U   U
   * Takes hash ID and TableInsertQuery as argument and updates the row of U   U
   * the given ID with the query                                           U   U
   *                                                                        UUU
   */
  bool updateQuery(const Hash& id, const Revision& query);
};

}

#endif  // TABLE_INTERFACE_H
