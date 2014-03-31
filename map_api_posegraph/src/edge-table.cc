/*
 * edge-table.cc
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#include <memory>
#include <vector>

#include <glog/logging.h>

#include <map_api_posegraph/edge-table.h>
#include "core.pb.h"

namespace map_api {
namespace posegraph {

bool EdgeTable::init(){
  return setup("posegraph_edge");
}

bool EdgeTable::define(){
  if (!addField("data",map_api::proto::TableFieldDescriptor_Type_BLOB))
    return false;
  return true;
}

map_api::Hash EdgeTable::insertEdge(const Edge &edge,
                                    FrameTable &frameTable){
  std::shared_ptr<map_api::TableInsertQuery> query = getTemplate();
  (*query)["data"]->set_blobvalue(edge.SerializeAsString());
  // commit
  map_api::Hash result = insertQuery(*query);
  if (!result.isValid()){
    return result;
  }
  // report to frames
  // from
  // TODO(discuss) OOOPS protobuf can at the time being only
  // give me a string back. I guess I'll just define a hash message and let
  // map_api::Hash extend it instead of this foul casting business. What do you
  // think?
  map_api::Hash fromId = map_api::Hash::cast(edge.from());
  std::shared_ptr<Frame> from = frameTable.get(fromId);
  from->add_outgoing(result.getString());
  frameTable.update(fromId, *from);
  /*
  // to
  map_api::Hash toId = map_api::Hash::cast(edge.to());
  std::shared_ptr<Frame> to = frameTable.get(toId);
  to->add_incoming(result.getString());
  frameTable.update(toId, *to);
  */
  // TODO(discuss) don't report to anchor frames (e.g. GPS)?
  return result;
}

} /* namespace posegraph */
} /* namespace map_api */
