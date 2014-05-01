/*
 * vertex-table.cc
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#include <glog/logging.h>

#include <map_api_posegraph/tables/vertex-table.h>

#include "posegraph.pb.h"

namespace map_api {

REVISION_PROTOBUF(posegraph::proto::Vertex)

namespace posegraph {

VertexTable::~VertexTable() {}

bool VertexTable::init(){
  return setup("posegraph_vertex_table");
}

bool VertexTable::define(){
  return addField<posegraph::proto::Vertex>("data");
}

} /* namespace posegraph */
} /* namespace map_api */
