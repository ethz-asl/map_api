#ifndef MAP_API_SPATIAL_INDEX_CELL_DATA_H_
#define MAP_API_SPATIAL_INDEX_CELL_DATA_H_

#include <multiagent-mapping-common/unique-id.h>

#include "./net-table.pb.h"

namespace map_api {
class PeerId;

class SpatialIndexCellData : public proto::SpatialIndexCellData {
 public:
  void addChunkIdIfNotPresent(const common::Id& id);
  void addListenerIfNotPresent(const PeerId& peer);

  // Add chunk ids FROM the cell data TO result.
  void addChunkIds(common::IdSet* result) const;
  void getListeners(std::unordered_set<PeerId>* result) const;
};

}  // namespace map_api

#endif  // MAP_API_SPATIAL_INDEX_CELL_DATA_H_
