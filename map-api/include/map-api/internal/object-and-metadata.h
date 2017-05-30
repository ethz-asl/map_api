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

#ifndef INTERNAL_OBJECT_AND_METADATA_H_
#define INTERNAL_OBJECT_AND_METADATA_H_

#include "map-api/net-table.h"
#include "map-api/revision.h"

namespace map_api {

// Splits a revision into object and metadata. Note that the revision stored
// in this object contains no custom field values, as these would be redundant
// with the Object; it only contains the metadata.
template <typename ObjectType>
struct ObjectAndMetadata {
  ObjectType object;
  std::shared_ptr<Revision> metadata;

  void deserialize(const Revision& source) {
    objectFromRevision(source, &object);
    source.copyForWrite(&metadata);
    metadata->clearCustomFieldValues();
  }

  void serialize(std::shared_ptr<const Revision>* destination) const {
    CHECK_NOTNULL(destination);
    std::shared_ptr<Revision> result;
    metadata->copyForWrite(&result);
    objectToRevision(object, result.get());
    *destination = result;
  }

  void createForInsert(const ObjectType& _object, NetTable* table) {
    CHECK_NOTNULL(table);
    object = _object;
    metadata = table->getTemplate();
  }
};

}  // namespace map_api

#endif  // INTERNAL_OBJECT_AND_METADATA_H_
