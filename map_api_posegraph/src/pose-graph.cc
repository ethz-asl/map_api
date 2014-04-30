/*
 * pose-graph.cc
 *
 *  Created on: Apr 30, 2014
 *      Author: titus
 */

#include <map_api_posegraph/pose-graph.h>
#include <map_api_posegraph/mission-adder.h>

namespace map_api {
namespace posegraph {

/* TODO(dymczykm, tcies) continue here
bool PoseGraph::saveToDatabase(
    std::shared_ptr<std::vector<sm::HashId> > vertices,
    std::shared_ptr<std::vector<sm::HashId> > odometryEdges,
    std::shared_ptr<std::vector<sm::HashId> > loopClosureEdges) const {
  MissionAdder adder;
  adder.begin();
  for (std::pair<pose_graph::VertexId, std::shared_ptr<pose_graph::Vertex> >
      vertex : vertices_){
    proto::Vertex vertexProto;
    vertexProto.set_time(0);
    vertexProto.set_dataformat("format");
    vertexProto.set_dataref("ref");

    adder << vertexProto;
  }
  adder.commit();
}

bool PoseGraph::loadFromDatabase(
    std::shared_ptr<const std::vector<sm::HashId> > vertices,
    std::shared_ptr<const std::vector<sm::HashId> > odometryEdges,
    std::shared_ptr<const std::vector<sm::HashId> > loopClosureEdges) {

}
*/

} /* namespace posegraph */
} /* namespace map_api */
