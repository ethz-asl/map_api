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

#include "map-api/internal/chunk-view.h"

#include "map-api/chunk-base.h"

namespace map_api {
namespace internal {

ChunkView::ChunkView(const ChunkBase& chunk, const LogicalTime& view_time)
    : chunk_(chunk), view_time_(view_time) {}

ChunkView::~ChunkView() {}

bool ChunkView::has(const map_api_common::Id& id) const {
  return static_cast<bool>(chunk_.constData()->getById(id, view_time_));
}

std::shared_ptr<const Revision> ChunkView::get(const map_api_common::Id& id) const {
  return chunk_.constData()->getById(id, view_time_);
}

void ChunkView::dump(ConstRevisionMap* result) const {
  chunk_.constData()->dump(view_time_, result);
}

void ChunkView::getAvailableIds(std::unordered_set<map_api_common::Id>* result) const {
  CHECK_NOTNULL(result)->clear();
  std::vector<map_api_common::Id> id_vector;
  chunk_.constData()->getAvailableIds(view_time_, &id_vector);
  for (const map_api_common::Id& id : id_vector) {
    result->emplace(id);
  }
}

void ChunkView::discardKnownUpdates(UpdateTimes* update_times) const {
  CHECK_NOTNULL(update_times);
  for (UpdateTimes::iterator it = update_times->begin();
       it != update_times->end();) {
    if (it->second <= view_time_) {
      it = update_times->erase(it);
    } else {
      ++it;
    }
  }
}

}  // namespace internal
}  // namespace map_api
