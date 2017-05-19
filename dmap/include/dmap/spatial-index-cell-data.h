#ifndef DMAP_SPATIAL_INDEX_CELL_DATA_H_
#define DMAP_SPATIAL_INDEX_CELL_DATA_H_

#include <dmap-common/unique-id.h>

#include "./net-table.pb.h"

namespace dmap {
class PeerId;

class SpatialIndexCellData : public proto::SpatialIndexCellData {
 public:
  void addChunkIdIfNotPresent(const dmap_common::Id& id);
  void addListenerIfNotPresent(const PeerId& peer);

  // Add chunk ids FROM the cell data TO result.
  void addChunkIds(dmap_common::IdSet* result) const;
  void getChunkIds(dmap_common::IdList* result) const;
  void getListeners(std::unordered_set<PeerId>* result) const;

  // False if both have the same chunk ids.
  bool chunkIdSetDiff(const SpatialIndexCellData& other,
                      dmap_common::IdList* result) const;
};

}  // namespace dmap

#endif  // DMAP_SPATIAL_INDEX_CELL_DATA_H_
