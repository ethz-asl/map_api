/*
 * edge-table.h
 *
 *  Created on: Apr 29, 2014
 *      Author: titus
 */

#ifndef ODOMETRY_EDGE_TABLE_H_
#define ODOMETRY_EDGE_TABLE_H_

#include <memory>

#include "map-api/cru-table-interface.h"

namespace map_api {
namespace posegraph {

// TODO(tcies) could this be a CR table? If yes, need to test CR Transactions!
class OdometryEdgeTable : public map_api::CRUTableInterface {
 public:
  virtual bool init();
 protected:
  virtual bool define();
};

} /* namespace posegraph */
} /* namespace map_api */

#endif /* ODOMETRY_EDGE_TABLE_H_ */
