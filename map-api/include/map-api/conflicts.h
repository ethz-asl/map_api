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

#ifndef MAP_API_CONFLICTS_H_
#define MAP_API_CONFLICTS_H_

#include <list>
#include <memory>
#include <sstream>  // NOLINT
#include <string>
#include <unordered_map>

#include "map-api/net-table.h"

namespace map_api {
class Revision;

struct Conflict {
  const std::shared_ptr<const Revision> theirs;
  const std::shared_ptr<const Revision> ours;
};

// Choosing list for constant splicing, linear iteration is fine.
class Conflicts : public std::list<Conflict> {};

class ConflictMap : public std::unordered_map<NetTable*, Conflicts> {
 public:
  inline std::string debugString() const {
    std::ostringstream ss;
    for (const value_type& pair : *this) {
      ss << pair.first->name() << ": " << pair.second.size() << " conflicts"
         << std::endl;
    }
    return ss.str();
  }

  // Requires specialization of
  // std::string getComparisonString(const ObjectType& a, const ObjectType& b);
  // or
  // std::string ObjectType::getComparisonString(const ObjectType&) const;
  // Note that the latter will be correctly called for shared pointers.
  template <typename ObjectType>
  std::string debugConflictsInTable(NetTable* table) {
    CHECK(table);
    iterator found = find(table);
    if (found == end()) {
      return "There are no conflicts in table " + table->name() + "!\n";
    }
    std::string result;
    for (const Conflict& conflict : found->second) {
      ObjectType our_object, their_object;
      objectFromRevision(*conflict.ours, &our_object);
      objectFromRevision(*conflict.theirs, &their_object);
      result += "For object " +
                conflict.ours->getId<map_api_common::Id>().printString() +
                " of table " + table->name() +
                " the attempted commit compares to the conflict as follows:\n" +
                getComparisonString(our_object, their_object) + "\n";
    }
    return result;
  }
};

}  // namespace map_api

#endif  // MAP_API_CONFLICTS_H_
