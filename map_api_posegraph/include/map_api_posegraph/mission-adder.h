/*
 * mission-adder.h
 *
 *  Created on: Apr 30, 2014
 *      Author: titus
 */

#ifndef MISSION_ADDER_H_
#define MISSION_ADDER_H_

#include <map-api/id.h>
#include <map-api/transaction.h>

#include "map_api_posegraph/tables/loop-closure-edge-table.h"
#include "map_api_posegraph/tables/odometry-edge-table.h"
#include "map_api_posegraph/tables/vertex-table.h"

namespace map_api {
namespace posegraph {

class MissionAdder : private map_api::Transaction {
 public:
  /**
   * Overrides Transaction::begin to also initialize table interfaces
   */
  bool begin();

  /**
   * Transaction operations to be accessible
   */
  using map_api::Transaction::abort;
  using map_api::Transaction::commit;

  /**
   * Adding Vertices and Edges
   */
  template<typename Datatype>
  map_api::Id operator <<(const Datatype& data);

 private:
  LoopClosureEdgeTable loopClosureTable_;
  OdometryEdgeTable odometryTable_;
  VertexTable vertexTable_;
};

} /* namespace posegraph */
} /* namespace map_api */

#endif /* MISSION_ADDER_H_ */
