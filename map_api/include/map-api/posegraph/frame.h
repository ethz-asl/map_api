/*
 * frame.h
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#ifndef FRAME_H_
#define FRAME_H_

#include "posegraph.pb.h"

namespace map_api {
namespace posegraph {

class Frame : public proto::Frame {
 public:
  Frame();
  virtual ~Frame();
};

} /* namespace posegraph */
} /* namespace map_api */

#endif /* FRAME_H_ */
