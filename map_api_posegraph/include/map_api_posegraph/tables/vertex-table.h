/*
 * vertex-table.h
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#ifndef VERTEX_TABLE_H_
#define VERTEX_TABLE_H_

#include "map-api/cru-table-interface.h"

namespace map_api {
namespace posegraph {

class VertexTable : public map_api::CRUTableInterface {
 public:
  VertexTable(const sm::HashId& owner);
  virtual bool init();
 protected:
  virtual bool define();
};

} /* namespace posegraph */
} /* namespace map_api */

#endif /* VERTEX_TABLE_H_ */
