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

#include <map-api/chunk-manager.h>

#include <utility>

#include <glog/logging.h>

#include "map-api/net-table.h"

namespace map_api {
void ChunkManagerBase::requestParticipationAllChunks() {
  if (active_chunks_.empty()) {
    return;
  }
  VLOG(3) << "Requesting participation for " << active_chunks_.size()
          << " chunks from " << underlying_table_->name();
  for (const std::pair<map_api_common::Id, ChunkBase*>& item : active_chunks_) {
    CHECK_NOTNULL(item.second);
    item.second->requestParticipation();
  }
  VLOG(3) << "Done. " << active_chunks_.size() << " chunks from "
          << underlying_table_->name() << " sent.";
}

ChunkBase* ChunkManagerChunkSize::getChunkForItem(const Revision& revision) {
  int item_size = revision.byteSize();
  int total_size = current_chunk_size_bytes_ + item_size;

  if (total_size > max_chunk_size_bytes_ || current_chunk_ == nullptr) {
    if (current_chunk_ != nullptr) {
      VLOG(3) << "New chunk size " << total_size
              << " larger than limit, creating a new chunk.";
    }
    current_chunk_ = underlying_table_->newChunk();
    active_chunks_.insert(std::make_pair(current_chunk_->id(), current_chunk_));
    current_chunk_size_bytes_ = 0;
  }
  CHECK_NOTNULL(current_chunk_);
  current_chunk_size_bytes_ += item_size;
  return current_chunk_;
}

}  // namespace map_api
