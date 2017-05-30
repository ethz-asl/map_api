// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

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
