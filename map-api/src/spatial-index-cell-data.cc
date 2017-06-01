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
// along with Map API. If not, see <http://www.gnu.org/licenses/>.

#include "map-api/spatial-index-cell-data.h"

#include <algorithm>

#include <glog/logging.h>

#include "map-api/peer-id.h"

namespace map_api {

void SpatialIndexCellData::addChunkIdIfNotPresent(const map_api_common::Id& id) {
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

void SpatialIndexCellData::addChunkIds(map_api_common::IdSet* result) const {
  CHECK_NOTNULL(result);
  for (int i = 0; i < chunk_ids_size(); ++i) {
    result->emplace(chunk_ids(i));
  }
}

void SpatialIndexCellData::getChunkIds(map_api_common::IdList* result) const {
  CHECK_NOTNULL(result)->clear();
  for (int i = 0; i < chunk_ids_size(); ++i) {
    result->push_back(map_api_common::Id(chunk_ids(i)));
  }
}

void SpatialIndexCellData::getListeners(std::unordered_set<PeerId>* result)
    const {
  CHECK_NOTNULL(result)->clear();
  for (int i = 0; i < listeners_size(); ++i) {
    result->emplace(PeerId(listeners(i)));
  }
}

bool SpatialIndexCellData::chunkIdSetDiff(const SpatialIndexCellData& other,
                                          map_api_common::IdList* result) const {
  map_api_common::IdList minuend, subtrahend;
  getChunkIds(&minuend);
  other.getChunkIds(&subtrahend);
  std::sort(minuend.begin(), minuend.end());
  std::sort(subtrahend.begin(), subtrahend.end());
  std::set_difference(minuend.begin(), minuend.end(), subtrahend.begin(),
                      subtrahend.end(), std::back_inserter(*result));
  return !result->empty();
}

}  // namespace map_api
