/*
 * loop-closure-edge-table.h
 *
 *  Created on: Apr 29, 2014
 *      Author: titus
 */

#ifndef LOOP_CLOSURE_EDGE_TABLE_H_
#define LOOP_CLOSURE_EDGE_TABLE_H_

#include <memory>

#include "map-api/cru-table-interface.h"

namespace map_api {
namespace posegraph {

class LoopClosureEdgeTable : public map_api::CRUTableInterface {
 public:
  virtual bool init();
 protected:
  virtual bool define();
};

} /* namespace posegraph */
} /* namespace map_api */

#endif /* LOOP_CLOSURE_EDGE_TABLE_H_ */
