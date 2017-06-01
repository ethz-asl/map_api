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

#ifndef MAP_API_WORKSPACE_H_
#define MAP_API_WORKSPACE_H_

#include <initializer_list>
#include <string>
#include <unordered_set>

#include "map-api/trackee-multimap.h"

namespace map_api {
class ChunkBase;
class Revision;

// See contains() implementation for precedence details.
// If a whitelist is empty, all items covered are considered whitelisted.
// Workspaces are only used to restrict reading operations, not writing
// operations.
class Workspace {
 public:
  Workspace(const std::initializer_list<NetTable*>& table_blacklist,
            const std::initializer_list<NetTable*>& table_whitelist);
  Workspace();

  // "table" is an input argument.
  bool contains(NetTable* table, const map_api_common::Id& chunk_id) const;
  bool contains(NetTable* table) const;

  // The chunk of the revision itself is also merged.
  void mergeRevisionTrackeesIntoBlacklist(const Revision& revision,
                                          NetTable* tracker_table);
  void mergeRevisionTrackeesIntoWhitelist(const Revision& revision,
                                          NetTable* tracker_table);

  class TableInterface {
   public:
    TableInterface(const Workspace& workspace, NetTable* table);
    bool contains(const map_api_common::Id& chunk_id) const;
    void forEachChunk(const std::function<void(const ChunkBase& chunk)>& action)
        const;

   private:
    const Workspace& workspace_;
    NetTable* table_;
  };

  std::string debugString() const;

 private:
  std::unordered_set<NetTable*> table_blacklist_;
  std::unordered_set<NetTable*> table_whitelist_;
  TrackeeMultimap chunk_blacklist_;
  TrackeeMultimap chunk_whitelist_;
};

}  // namespace map_api

#endif  // MAP_API_WORKSPACE_H_
