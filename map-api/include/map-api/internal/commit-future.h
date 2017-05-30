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

#ifndef INTERNAL_COMMIT_FUTURE_H_
#define INTERNAL_COMMIT_FUTURE_H_

#include "map-api/internal/overriding-view-base.h"
#include "map-api/revision-map.h"

namespace map_api {
class ChunkTransaction;

namespace internal {

// Can replace a ChunkView in a transaction to signify that that transaction
// depends on another transaction that is committing in parallel.
class CommitFuture : public ViewBase {
 public:
  explicit CommitFuture(
      const ChunkTransaction& finalized_committing_transaction);
  explicit CommitFuture(const CommitFuture& other);
  ~CommitFuture();

  // ==================
  // VIEWBASE INTERFACE
  // ==================
  virtual bool has(const map_api_common::Id& id) const override;
  virtual std::shared_ptr<const Revision> get(const map_api_common::Id& id) const
      override;
  virtual void dump(ConstRevisionMap* result) const override;
  virtual void getAvailableIds(std::unordered_set<map_api_common::Id>* result) const
      override;
  virtual void discardKnownUpdates(UpdateTimes* update_times) const override;

 private:
  ConstRevisionMap chunk_state_;
};

}  // namespace internal
}  // namespace map_api

#endif  // INTERNAL_COMMIT_FUTURE_H_
