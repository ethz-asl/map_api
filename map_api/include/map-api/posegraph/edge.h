/*
 * edge.h
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#ifndef EDGE_H_
#define EDGE_H_

#include "posegraph.pb.h"

namespace map_api {
namespace posegraph {

class Edge : public proto::Edge {
 public:
  Edge();
  virtual ~Edge();
};

} /* namespace posegraph */
} /* namespace map_api */

#endif /* EDGE_H_ */
