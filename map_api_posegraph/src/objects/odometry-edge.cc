/*
 * odometry-edge.cc
 *
 *  Created on: Apr 30, 2014
 *      Author: titus
 */

#include <map_api_posegraph/objects/odometry-edge.h>

namespace map_api {
namespace posegraph {

OdometryEdge::OdometryEdge(const sm::HashId& from, const sm::HashId& to,
                           double translation[], double rotation[],
                           double covMat[]){
  proto::Edge& edge(*mutable_edge());
  edge.set_from(from.hexString());
  edge.set_to(to.hexString());
  for (int i = 0; i < 3; ++i){
    edge.add_translation(translation[i]);
  }
  for (int i = 0; i < 4; ++i){
    edge.add_rotation(rotation[i]);
  }
  for (int i = 0; i < 36; ++i){
    edge.add_covmat(covMat[i]);
  }
}

} // namespace posegraph
} /* namespace map_api */
