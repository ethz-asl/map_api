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

#ifndef DMAP_LEGACY_CHUNK_DATA_CONTAINER_BASE_INL_H_
#define DMAP_LEGACY_CHUNK_DATA_CONTAINER_BASE_INL_H_

namespace map_api {

LegacyChunkDataContainerBase::History::const_iterator
LegacyChunkDataContainerBase::History::latestAt(const LogicalTime& time) const {
  for (const_iterator it = cbegin(); it != cend(); ++it) {
    if ((*it)->getUpdateTime() <= time) {
      if ((*it)->isRemoved()) {
        return cend();
      } else {
        return it;
      }
    }
  }
  return cend();
}

template <typename IdType>
void LegacyChunkDataContainerBase::itemHistory(const IdType& id,
                                               const LogicalTime& time,
                                               History* dest) const {
  map_api_common::Id map_api_id;
  map_api_common::HashId hash_id;
  id.toHashId(&hash_id);
  map_api_id.fromHashId(hash_id);
  itemHistoryImpl(map_api_id, time, dest);
}

template <typename ValueType>
void LegacyChunkDataContainerBase::findHistory(int key, const ValueType& value,
                                               const LogicalTime& time,
                                               HistoryMap* dest) const {
  std::shared_ptr<Revision> valueHolder = this->getTemplate();
  if (key >= 0) {
    valueHolder->set(key, value);
  }
  return this->findHistoryByRevision(key, *valueHolder, time, dest);
}

template <typename IdType>
void LegacyChunkDataContainerBase::remove(const LogicalTime& time,
                                          const IdType& id) {
  std::shared_ptr<Revision> latest =
      std::make_shared<Revision>(*getById(id, time));
  remove(time, latest);
}

}  // namespace map_api

#endif  // DMAP_LEGACY_CHUNK_DATA_CONTAINER_BASE_INL_H_
