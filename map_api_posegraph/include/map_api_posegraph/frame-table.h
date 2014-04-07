/*
 * frame-table.h
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#ifndef FRAME_TABLE_H_
#define FRAME_TABLE_H_

#include "map-api/cru-table-interface.h"
#include "map_api_posegraph/frame.h"

namespace map_api {
namespace posegraph {

// TODO(tcies) implement generic class for blob tables?
class FrameTable : public map_api::CRUTableInterface {
 public:
  virtual bool init();
  map_api::Hash insertFrame(const Frame& frame);
  std::shared_ptr<Frame> get(const map_api::Hash &id);
  bool update(const map_api::Hash& hash, const Frame& frame);
 protected:
  virtual bool define();
};

} /* namespace posegraph */
} /* namespace map_api */

#endif /* FRAME_TABLE_H_ */
