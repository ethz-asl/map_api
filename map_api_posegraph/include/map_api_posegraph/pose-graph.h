/*
 * pose-graph.h
 *
 *  Created on: Apr 30, 2014
 *      Author: titus
 */

#ifndef MAP_API_POSE_GRAPH_H_
#define MAP_API_POSE_GRAPH_H_

#include <vector>

#include <aslam_posegraph/switchable-constraints-pose-graph.h>

#include <sm/hash_id.hpp>

namespace map_api {
namespace posegraph {

class PoseGraph : public ::pose_graph::SwitchableConstraintsPoseGraph {
  // TODO(tcies) this does NOT maintain the same ID's in the database
  bool saveToDatabase(
      std::shared_ptr<std::vector<sm::HashId> > vertices,
      std::shared_ptr<std::vector<sm::HashId> > odometryEdges,
      std::shared_ptr<std::vector<sm::HashId> > loopClosureEdges) const;
  bool loadFromDatabase(
      std::shared_ptr<const std::vector<sm::HashId> > vertices,
      std::shared_ptr<const std::vector<sm::HashId> > odometryEdges,
      std::shared_ptr<const std::vector<sm::HashId> > loopClosureEdges);
};

} /* namespace posegraph */
} /* namespace map_api */

#endif /* MAP_API_POSE_GRAPH_H_ */
