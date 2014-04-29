/*
 * edge-table.h
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#ifndef EDGE_TABLE_H_
#define EDGE_TABLE_H_

#include <memory>

#include "map-api/cru-table-interface.h"
#include "map_api_posegraph/edge.h"
#include "map_api_posegraph/frame-table.h"

namespace map_api {
namespace posegraph {

/* TODO(tcies) adapt to transaction-centricity

class EdgeTable : public map_api::CRUTableInterface {
 public:
  virtual bool init();
  map_api::Hash insertEdge(const Edge &edge, FrameTable &frameTable);
 protected:
  virtual bool define();
};

*/

} /* namespace posegraph */
} /* namespace map_api */

#endif /* EDGE_TABLE_H_ */
