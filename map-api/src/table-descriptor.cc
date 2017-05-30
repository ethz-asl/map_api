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

#include "map-api/table-descriptor.h"

#include <glog/logging.h>

#include "map-api/revision.h"

namespace map_api {

TableDescriptor::~TableDescriptor() {}

void TableDescriptor::addField(int index, proto::Type type) {
  CHECK_EQ(fields_size(), index) << "Fields must be added in-order";
  add_fields(type);
}

void TableDescriptor::setName(const std::string& name) { set_name(name); }

void TableDescriptor::setSpatialIndex(const SpatialIndex::BoundingBox& extent,
                                      const std::vector<size_t>& subdivision) {
  CHECK_EQ(subdivision.size(), extent.size());
  extent.serialize(mutable_spatial_extent());
  clear_spatial_subdivision();
  for (size_t dimension_division : subdivision) {
    add_spatial_subdivision(dimension_division);
  }
}

std::shared_ptr<Revision> TableDescriptor::getTemplate() const {
  std::shared_ptr<Revision> result;
  Revision::fromProto(std::unique_ptr<proto::Revision>(new proto::Revision),
                      &result);
  // add editable fields
  for (int i = 0; i < fields_size(); ++i) {
    result->addField(i, fields(i));
  }
  return result;
}

} // namespace map_api
