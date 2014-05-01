#include "map_api_posegraph/mission-adder.h"

#include <memory>

#include <glog/logging.h>

#include "map-api/revision.h"

#include "posegraph.pb.h"

namespace map_api {
namespace posegraph {

bool MissionAdder::begin() {
  map_api::CRTableInterface* tables[] = {&loopClosureTable_, &odometryTable_,
      &vertexTable_};
  for (map_api::CRTableInterface* table : tables){
    if (!table->init()){
      LOG(FATAL) << "Failed to initialize table " << table->name();
    }
  }
  return map_api::Transaction::begin();
}

template<>
map_api::Id MissionAdder::operator <<<posegraph::proto::LoopClosureEdge>(
    const posegraph::proto::LoopClosureEdge& data){
  std::shared_ptr<map_api::Revision> toInsert = loopClosureTable_.getTemplate();
  toInsert->set("data", data);
  return insert(loopClosureTable_, toInsert);
}

template<>
map_api::Id MissionAdder::operator <<<posegraph::proto::OdometryEdge>(
    const posegraph::proto::OdometryEdge& data){
  std::shared_ptr<map_api::Revision> toInsert = odometryTable_.getTemplate();
  toInsert->set("data", data);
  return insert(odometryTable_, toInsert);
}

template<>
map_api::Id MissionAdder::operator <<<posegraph::proto::Vertex>(
    const posegraph::proto::Vertex& data){
  std::shared_ptr<map_api::Revision> toInsert = vertexTable_.getTemplate();
  toInsert->set("data", data);
  return insert(vertexTable_, toInsert);
}

} /* namespace posegraph */
} /* namespace map_api */
