/*
 * odometry-edge.h
 *
 *  Created on: Apr 30, 2014
 *      Author: titus
 */

#ifndef ODOMETRY_EDGE_H_
#define ODOMETRY_EDGE_H_

#include "sm/hash_id.hpp"

#include "posegraph.pb.h"

namespace map_api {

namespace posegraph {

class OdometryEdge : public proto::OdometryEdge {
  OdometryEdge(const sm::HashId& from, const sm::HashId& to,
               double translation[], double rotation[], double covMat[]);
};

} /* namespace posegraph */
} /* namespace map_api */

#endif /* ODOMETRY_EDGE_H_ */
