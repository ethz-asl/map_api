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

#include "map-api/internal/combined-view.h"

#include <glog/logging.h>

#include "map-api/revision-map.h"

namespace map_api {
namespace internal {

CombinedView::CombinedView(const std::unique_ptr<ViewBase>& complete_view,
                           const OverridingViewBase& override_view)
    : complete_view_(complete_view), override_view_(override_view) {}

CombinedView::~CombinedView() {}

bool CombinedView::has(const map_api_common::Id& id) const {
  return override_view_.has(id) ||
         (complete_view_->has(id) && !override_view_.suppresses(id));
}

std::shared_ptr<const Revision> CombinedView::get(const map_api_common::Id& id) const {
  if (override_view_.has(id)) {
    return override_view_.get(id);
  } else {
    if (complete_view_->has(id)) {
      if (!override_view_.suppresses(id)) {
        return complete_view_->get(id);
      }
    }
  }

  return std::shared_ptr<const Revision>();
}

void CombinedView::dump(ConstRevisionMap* result) const {
  CHECK_NOTNULL(result)->clear();

  ConstRevisionMap override_dump;
  override_view_.dump(&override_dump);

  complete_view_->dump(result);
  for (ConstRevisionMap::iterator it = result->begin(); it != result->end();) {
    if (override_view_.suppresses(it->first)) {
      it = result->erase(it);
    } else {
      ++it;
    }
  }

  result->insert(override_dump.begin(), override_dump.end());
}

void CombinedView::getAvailableIds(std::unordered_set<map_api_common::Id>* result)
    const {
  CHECK_NOTNULL(result)->clear();
  override_view_.getAvailableIds(result);

  std::unordered_set<map_api_common::Id> all_unsupressed_ids;
  complete_view_->getAvailableIds(&all_unsupressed_ids);
  for (std::unordered_set<map_api_common::Id>::iterator it =
           all_unsupressed_ids.begin();
       it != all_unsupressed_ids.end();) {
    if (override_view_.suppresses(*it)) {
      it = all_unsupressed_ids.erase(it);
    } else {
      ++it;
    }
  }
}

void CombinedView::discardKnownUpdates(UpdateTimes* update_times) const {
  CHECK_NOTNULL(update_times);
  override_view_.discardKnownUpdates(update_times);
  complete_view_->discardKnownUpdates(update_times);
}

}  // namespace internal
}  // namespace map_api
