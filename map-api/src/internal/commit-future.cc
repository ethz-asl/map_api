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

#include "map-api/internal/commit-future.h"

#include "map-api/chunk-transaction.h"

namespace map_api {
namespace internal {

CommitFuture::CommitFuture(
    const ChunkTransaction& finalized_committing_transaction) {
  CHECK(finalized_committing_transaction.finalized_)
      << "Base transaction of commit future must be finalized.";
  const DeltaView& finalized_delta = finalized_committing_transaction.delta_;
  std::unique_ptr<ViewBase> current_view(new ChunkView(
      *finalized_committing_transaction.chunk_, LogicalTime::sample()));
  CombinedView future_view(current_view, finalized_delta);
  future_view.dump(&chunk_state_);
}

CommitFuture::CommitFuture(const CommitFuture& other)
    : chunk_state_(other.chunk_state_) {}

CommitFuture::~CommitFuture() {}

bool CommitFuture::has(const map_api_common::Id& id) const {
  return static_cast<bool>(get(id));
}

std::shared_ptr<const Revision> CommitFuture::get(const map_api_common::Id& id) const {
  ConstRevisionMap::const_iterator found = chunk_state_.find(id);
  if (found != chunk_state_.end()) {
    return found->second;
  } else {
    return std::shared_ptr<const Revision>();
  }
}

void CommitFuture::dump(ConstRevisionMap* result) const {
  CHECK_NOTNULL(result)->insert(chunk_state_.begin(), chunk_state_.end());
}

void CommitFuture::getAvailableIds(std::unordered_set<map_api_common::Id>* result)
    const {
  CHECK_NOTNULL(result)->clear();
  for (const ConstRevisionMap::value_type& id_revision : chunk_state_) {
    result->emplace(id_revision.first);
  }
}

void CommitFuture::discardKnownUpdates(UpdateTimes* update_times) const {
  LOG(FATAL) << "Detach futures from chunk transaction before committing!";
}

}  // namespace internal
}  // namespace map_api
