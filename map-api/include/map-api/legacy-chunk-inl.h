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

#ifndef MAP_API_LEGACY_CHUNK_INL_H_
#define MAP_API_LEGACY_CHUNK_INL_H_

#include "map-api/chunk-data-container-base.h"

namespace map_api {

template <typename RequestType>
void LegacyChunk::fillMetadata(RequestType* destination) const {
  CHECK_NOTNULL(destination);
  destination->mutable_metadata()->set_table(this->data_container_->name());
  id().serialize(destination->mutable_metadata()->mutable_chunk_id());
}

inline void LegacyChunk::syncLatestCommitTime(const Revision& item) {
  LogicalTime commit_time = item.getModificationTime();
  if (commit_time > latest_commit_time_) {
    latest_commit_time_ = commit_time;
  }
}

}  // namespace map_api

#endif  // MAP_API_LEGACY_CHUNK_INL_H_
