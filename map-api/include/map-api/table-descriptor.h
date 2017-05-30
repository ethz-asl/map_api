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

#ifndef DMAP_TABLE_DESCRIPTOR_H_
#define DMAP_TABLE_DESCRIPTOR_H_

#include <string>
#include <vector>

#include "map-api/spatial-index.h"
#include "./core.pb.h"

namespace map_api {
class Revision;

class TableDescriptor : private proto::TableDescriptor {
  friend class ChunkDataContainerBase;
  friend class NetTableManager;
  friend class Revision;

 public:
  virtual ~TableDescriptor();

  using proto::TableDescriptor::name;

  template <typename Type>
  void addField(int index);
  void addField(int index, proto::Type type);

  void setName(const std::string& name);

  void setSpatialIndex(const SpatialIndex::BoundingBox& extent,
                       const std::vector<size_t>& subdivision);

  std::shared_ptr<Revision> getTemplate() const;
};

}  // namespace map_api

#include "./table-descriptor-inl.h"

#endif  // DMAP_TABLE_DESCRIPTOR_H_
