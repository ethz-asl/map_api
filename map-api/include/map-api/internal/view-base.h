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

#ifndef INTERNAL_VIEW_BASE_H_
#define INTERNAL_VIEW_BASE_H_

#include <memory>
#include <unordered_map>
#include <unordered_set>

namespace map_api_common {
class Id;
}  // namespace common

namespace map_api {
class ConstRevisionMap;
class LogicalTime;
class Revision;

namespace internal {

class ViewBase {
 public:
  virtual ~ViewBase();

  virtual bool has(const map_api_common::Id& id) const = 0;
  virtual std::shared_ptr<const Revision> get(const map_api_common::Id& id) const = 0;
  virtual void dump(ConstRevisionMap* result) const = 0;
  virtual void getAvailableIds(std::unordered_set<map_api_common::Id>* result)
      const = 0;

  // From the supplied update times, discard the ones whose updates are
  // incorporated by the view.
  typedef std::unordered_map<map_api_common::Id, LogicalTime> UpdateTimes;
  virtual void discardKnownUpdates(UpdateTimes* update_times) const = 0;
};

}  // namespace internal
}  // namespace map_api

#endif  // INTERNAL_VIEW_BASE_H_
