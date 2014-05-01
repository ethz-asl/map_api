/*
 * loop-closure-edge-table.cc
 *
 *  Created on: Apr 29, 2014
 *      Author: titus
 */

#include <map_api_posegraph/tables/loop-closure-edge-table.h>

#include <memory>
#include <vector>

#include <glog/logging.h>
#include <map-api/revision.h>

#include "core.pb.h"
#include "posegraph.pb.h"

namespace map_api {

REVISION_PROTOBUF(posegraph::proto::LoopClosureEdge)

namespace posegraph {

bool LoopClosureEdgeTable::init(){
  return setup("posegraph_loop_closure_edge_table");
}

bool LoopClosureEdgeTable::define(){
  return addField<posegraph::proto::LoopClosureEdge>("data");
}

} /* namespace posegraph */
} /* namespace map_api */
