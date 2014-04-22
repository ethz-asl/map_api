/*
 * frame-table.cc
 *
 *  Created on: Mar 17, 2014
 *      Author: titus
 */

#include <glog/logging.h>

#include <map_api_posegraph/frame-table.h>

namespace map_api {

//TODO(tcies) in definitive version of map api posegraph: Move these to
// separate file, e.g. table-field-extension.cc (?)
REVISION_ENUM(posegraph::Frame, proto::TableFieldDescriptor_Type_BLOB)

REVISION_SET(posegraph::Frame){
  field.set_blobvalue(value.SerializeAsString());
  return true;
}

REVISION_GET(posegraph::Frame){
  bool parsed = value->ParseFromString(field.blobvalue());
  if (!parsed) {
    LOG(ERROR) << "Failed to parse Frame";
    return false;
  }
  return true;
}

namespace posegraph {


/** TODO(tcies) outdated, adapt to transaction-centricity
bool FrameTable::init(){
  return setup("posegraph_frame");
}

bool FrameTable::define(){
  if (!addField<Frame>("data"))
    return false;
  return true;
}

map_api::Hash FrameTable::insertFrame(const Frame &frame){
  std::shared_ptr<map_api::Revision> query = getTemplate();
  query->set("data", frame);
  // commit
  return insertQuery(*query);
}

std::shared_ptr<Frame> FrameTable::get(const map_api::Hash &id){
  std::shared_ptr<map_api::Revision> result = getRow(id);
  return std::make_shared<Frame>(result->get<Frame>("data"));
}

// TODO(tcies) many similarities with insert... meldable?
bool FrameTable::update(const map_api::Hash& hash, const Frame& frame){
  std::shared_ptr<map_api::Revision> query = getTemplate();
  query->set("data", frame);
  return updateQuery(hash, *query);
}
*/

} /* namespace posegraph */
} /* namespace map_api */
