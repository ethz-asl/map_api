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

#ifndef MAP_API_REVISION_MAP_INL_H_
#define MAP_API_REVISION_MAP_INL_H_

#include <memory>
#include <unordered_map>
#include <utility>

namespace map_api {

template <typename RevisionType>
template <typename Derived>
typename RevisionMapBase<RevisionType>::iterator
RevisionMapBase<RevisionType>::find(const map_api_common::UniqueId<Derived>& key) {
  map_api_common::Id id_key;
  map_api_common::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);
  return find(id_key);
}

template <typename RevisionType>
template <typename Derived>
typename RevisionMapBase<RevisionType>::const_iterator RevisionMapBase<
    RevisionType>::find(const map_api_common::UniqueId<Derived>& key) const {
  map_api_common::Id id_key;
  map_api_common::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);  // TODO(tcies) avoid conversion? how?
  return find(id_key);
}

template <typename RevisionType>
std::pair<typename RevisionMapBase<RevisionType>::iterator, bool>
RevisionMapBase<RevisionType>::insert(
    const std::shared_ptr<RevisionType>& revision) {
  CHECK_NOTNULL(revision.get());
  return insert(
      std::make_pair(revision->template getId<map_api_common::Id>(), revision));
}

template <typename RevisionType>
template <typename Derived>
std::pair<typename RevisionMapBase<RevisionType>::iterator, bool>
RevisionMapBase<RevisionType>::insert(
    const map_api_common::UniqueId<Derived>& key,
    const std::shared_ptr<RevisionType>& revision) {
  map_api_common::Id id_key;
  map_api_common::HashId hash_id;
  key.toHashId(&hash_id);
  id_key.fromHashId(hash_id);  // TODO(tcies) avoid conversion? how?
  return insert(std::make_pair(id_key, revision));
}

}  // namespace map_api

#endif  // MAP_API_REVISION_MAP_INL_H_
