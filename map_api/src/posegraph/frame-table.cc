/*
 * frame-table.cc
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#include <glog/logging.h>

#include <map-api/posegraph/frame-table.h>

namespace map_api {
namespace posegraph {

bool FrameTable::init(){
  return setup("posegraph_frame");
}

bool FrameTable::define(){
  if (!addField("data",map_api::proto::TableFieldDescriptor_Type_BLOB))
    return false;
  return true;
}

map_api::Hash FrameTable::insertFrame(const Frame &frame){
  std::shared_ptr<map_api::TableInsertQuery> query = getTemplate();
  (*query)["data"]->set_blobvalue(frame.SerializeAsString());
  // commit
  return insertQuery(*query);
}

std::shared_ptr<Frame> FrameTable::get(const map_api::Hash &id){
  std::shared_ptr<map_api::TableInsertQuery> result = getRow(id);
  std::shared_ptr<Frame> ret(new Frame());
  ret->ParseFromString((*result)["data"]->blobvalue());
  return ret;
}

// TODO(tcies) many similarities with insert... meldable?
bool FrameTable::update(const map_api::Hash& hash, const Frame& frame){
  std::shared_ptr<map_api::TableInsertQuery> query = getTemplate();
  (*query)["data"]->set_blobvalue(frame.SerializeAsString());
  // commit
  return updateQuery(hash, *query);
}

} /* namespace posegraph */
} /* namespace map_api */
