/*
 * edge-table.h
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#ifndef EDGE_TABLE_H_
#define EDGE_TABLE_H_

#include <memory>

#include "map-api/table-interface.h"
#include "map-api/hash.h"
#include "map_api_posegraph/edge.h"
#include "map_api_posegraph/frame-table.h"

namespace map_api {
namespace posegraph {

class EdgeTable : public map_api::TableInterface {
 public:
  virtual bool init();
  /**
   * TODO (discuss) instead of passing around table references and initializing
   * them by the user, implement as Singleton pattern?
   */
  map_api::Hash insertEdge(const Edge &edge, FrameTable &frameTable);
 protected:
  virtual bool define();
};

} /* namespace posegraph */
} /* namespace map_api */

#endif /* EDGE_TABLE_H_ */
