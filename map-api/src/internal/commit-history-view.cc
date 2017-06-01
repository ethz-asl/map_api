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

#include "map-api/internal/commit-history-view.h"

#include "map-api/chunk-base.h"
#include "map-api/logical-time.h"

namespace map_api {
namespace internal {

CommitHistoryView::CommitHistoryView(const History& commit_history,
                                     const ChunkBase& chunk)
    : commit_history_(commit_history), chunk_(chunk) {}

CommitHistoryView::~CommitHistoryView() {}

bool CommitHistoryView::has(const map_api_common::Id& id) const {
  return static_cast<bool>(get(id));
}

std::shared_ptr<const Revision> CommitHistoryView::get(const map_api_common::Id& id)
    const {
  // Item could be deleted, so we need to attempt to get it.
  History::const_iterator found = commit_history_.find(id);
  if (found != commit_history_.end()) {
    return chunk_.constData()->getById(id, found->second);
  } else {
    return std::shared_ptr<const Revision>();
  }
}

void CommitHistoryView::dump(ConstRevisionMap* result) const {
  CHECK_NOTNULL(result)->clear();
  for (const History::value_type& history_item : commit_history_) {
    std::shared_ptr<const Revision> item = get(history_item.first);
    if (item) {
      result->emplace(history_item.first, item);
    }
  }
}

void CommitHistoryView::getAvailableIds(std::unordered_set<map_api_common::Id>* result)
    const {
  CHECK_NOTNULL(result)->clear();
  for (const History::value_type& history_item : commit_history_) {
    if (has(history_item.first)) {
      result->emplace(history_item.first);
    }
  }
}

void CommitHistoryView::discardKnownUpdates(UpdateTimes* update_times) const {
  CHECK_NOTNULL(update_times);
  for (const History::value_type& known_update : commit_history_) {
    UpdateTimes::const_iterator found = update_times->find(known_update.first);
    if (found != update_times->end()) {
      if (found->second <= known_update.second) {
        update_times->erase(found);
      }
    }
  }
}

bool CommitHistoryView::suppresses(const map_api_common::Id& id) const {
  History::const_iterator found = commit_history_.find(id);
  if (found != commit_history_.end()) {
    if (!chunk_.constData()->getById(id, found->second)) {
      return true;
    }
  }
  return false;
}

}  // namespace internal
}  // namespace map_api
