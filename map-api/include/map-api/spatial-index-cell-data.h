#ifndef DMAP_SPATIAL_INDEX_CELL_DATA_H_
#define DMAP_SPATIAL_INDEX_CELL_DATA_H_

#include <map-api-common/unique-id.h>

#include "./net-table.pb.h"

namespace map_api {
class PeerId;

class SpatialIndexCellData : public proto::SpatialIndexCellData {
 public:
  void addChunkIdIfNotPresent(const map_api_common::Id& id);
  void addListenerIfNotPresent(const PeerId& peer);

  // Add chunk ids FROM the cell data TO result.
  void addChunkIds(map_api_common::IdSet* result) const;
  void getChunkIds(map_api_common::IdList* result) const;
  void getListeners(std::unordered_set<PeerId>* result) const;

  // False if both have the same chunk ids.
  bool chunkIdSetDiff(const SpatialIndexCellData& other,
                      map_api_common::IdList* result) const;
};

}  // namespace map_api

#endif  // DMAP_SPATIAL_INDEX_CELL_DATA_H_
