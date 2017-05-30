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

#ifndef DMAP_REVISION_MAP_H_
#define DMAP_REVISION_MAP_H_

#include <memory>
#include <unordered_map>
#include <utility>

#include <map-api-common/unique-id.h>

#include "map-api/revision.h"

namespace map_api {

template <typename ConstNonConstRevision>
class RevisionMapBase
    : public std::unordered_map<map_api_common::Id,
                                std::shared_ptr<ConstNonConstRevision>> {
 public:
  typedef std::unordered_map<
      map_api_common::Id, std::shared_ptr<ConstNonConstRevision>>
      Base;
  typedef typename Base::iterator iterator;
  typedef typename Base::const_iterator const_iterator;

  using Base::find;
  template <typename IdType>
  iterator find(const map_api_common::UniqueId<IdType>& key);
  template <typename IdType>
  const_iterator find(const map_api_common::UniqueId<IdType>& key) const;

  using Base::insert;
  std::pair<iterator, bool> insert(
      const std::shared_ptr<ConstNonConstRevision>& revision);
  template <typename IdType>
  std::pair<typename Base::iterator, bool> insert(
      const map_api_common::UniqueId<IdType>& key,
      const std::shared_ptr<ConstNonConstRevision>& revision);
};

// Using derived classes here allows forward declaration.
class ConstRevisionMap : public RevisionMapBase<const Revision> {};
class MutableRevisionMap : public RevisionMapBase<Revision> {};

}  // namespace map_api

#include "map-api/revision-map-inl.h"

#endif  // DMAP_REVISION_MAP_H_
