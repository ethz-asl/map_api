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

#ifndef INTERNAL_COMMIT_HISTORY_VIEW_H_
#define INTERNAL_COMMIT_HISTORY_VIEW_H_

#include <unordered_map>

#include "map-api/internal/overriding-view-base.h"

namespace map_api {
class ChunkBase;
class LogicalTime;

namespace internal {

class CommitHistoryView : public OverridingViewBase {
 public:
  typedef std::unordered_map<map_api_common::Id, LogicalTime> History;

  CommitHistoryView(const History& commit_history, const ChunkBase& chunk);
  ~CommitHistoryView();

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

  // ============================
  // OVERRIDINGVIEWBASE INTERFACE
  // ============================
  virtual bool suppresses(const map_api_common::Id& id) const override;

 private:
  const std::unordered_map<map_api_common::Id, LogicalTime>& commit_history_;
  const ChunkBase& chunk_;
};

}  // namespace internal
}  // namespace map_api

#endif  // INTERNAL_COMMIT_HISTORY_VIEW_H_
