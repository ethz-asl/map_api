/*
 * odometry-edge-table.cc
 *
 *  Created on: Apr 29, 2014
 *      Author: titus
 */

#include <map_api_posegraph/tables/odometry-edge-table.h>

#include <memory>
#include <vector>

#include <glog/logging.h>
#include <map-api/revision.h>

#include "core.pb.h"
#include "posegraph.pb.h"

namespace map_api {

REVISION_PROTOBUF(posegraph::proto::OdometryEdge)

namespace posegraph {

bool OdometryEdgeTable::init(){
  return setup("posegraph_odometry_edge_table");
}

bool OdometryEdgeTable::define(){
  return addField<posegraph::proto::OdometryEdge>("data");
}

} /* namespace posegraph */
} /* namespace map_api */
