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

#ifndef DMAP_CHORD_INDEX_INL_H_
#define DMAP_CHORD_INDEX_INL_H_

#include <cstring>
#include <vector>

#include <Poco/MD5Engine.h>
#include <Poco/DigestStream.h>

namespace map_api {
template <typename DataType>
ChordIndex::Key ChordIndex::hash(const DataType& data) {
  static_assert(sizeof(Key) <= sizeof(size_t),
                "Key should be smaller than std::hash() output!");
  union KeyUnion {
    Key key;
    size_t hash_result;
  };

  KeyUnion key_union;
  key_union.hash_result = std::hash<DataType>()(data);
  return key_union.key;
}
}  // namespace map_api

#endif  // DMAP_CHORD_INDEX_INL_H_
