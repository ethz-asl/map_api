#include "map-api/spatial-index-cell-data.h"

#include <glog/logging.h>

#include "map-api/peer-id.h"

namespace map_api {

void SpatialIndexCellData::addChunkIdIfNotPresent(const common::Id& id) {
  for (int i = 0; i < chunk_ids_size(); ++i) {
    if (id.correspondsTo(chunk_ids(i))) {
      return;
    }
  }
  id.serialize(add_chunk_ids());
}

void SpatialIndexCellData::addListenerIfNotPresent(const PeerId& peer) {
  for (int i = 0; i < listeners_size(); ++i) {
    if (peer.ipPort() == listeners(i)) {
      return;
    }
  }
  add_listeners(peer.ipPort());
}

void SpatialIndexCellData::addChunkIds(common::IdSet* result) const {
  CHECK_NOTNULL(result);
  for (int i = 0; i < chunk_ids_size(); ++i) {
    result->emplace(common::Id(chunk_ids(i)));
  }
}

void SpatialIndexCellData::getListeners(std::unordered_set<PeerId>* result)
    const {
  CHECK_NOTNULL(result)->clear();
  for (int i = 0; i < listeners_size(); ++i) {
    result->emplace(PeerId(listeners(i)));
  }
}

}  // namespace map_api
