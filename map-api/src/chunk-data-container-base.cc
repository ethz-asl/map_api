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

#include <cstdio>
#include <map>

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <map-api/chunk-data-container-base.h>

#include "./core.pb.h"
#include <map-api/core.h>

namespace map_api {

ChunkDataContainerBase::ChunkDataContainerBase() : initialized_(false) {}

ChunkDataContainerBase::~ChunkDataContainerBase() {}

bool ChunkDataContainerBase::init(std::shared_ptr<TableDescriptor> descriptor) {
  CHECK(descriptor);
  CHECK(descriptor->has_name());
  descriptor_ = descriptor;
  CHECK(initImpl());
  initialized_ = true;
  return true;
}

bool ChunkDataContainerBase::isInitialized() const { return initialized_; }

const std::string& ChunkDataContainerBase::name() const {
  return descriptor_->name();
}

std::shared_ptr<Revision> ChunkDataContainerBase::getTemplate() const {
  CHECK(isInitialized()) << "Can't get template of non-initialized table";
  return descriptor_->getTemplate();
}

void ChunkDataContainerBase::findByRevision(int key,
                                            const Revision& valueHolder,
                                            const LogicalTime& time,
                                            ConstRevisionMap* dest) const {
  std::lock_guard<std::mutex> lock(access_mutex_);
  CHECK(isInitialized()) << "Attempted to find in non-initialized table";
  // whether valueHolder contains key is implicitly checked whenever using
  // Revision::insertPlaceHolder - for now it's a pretty safe bet that the
  // implementation uses that - this would be rather cumbersome to check here
  CHECK_NOTNULL(dest);
  dest->clear();
  CHECK(time < LogicalTime::sample())
      << "Seeing the future is yet to be implemented ;)";
  findByRevisionImpl(key, valueHolder, time, dest);
}

int ChunkDataContainerBase::numAvailableIds(const LogicalTime& time) const {
  return count(-1, 0, time);
}

int ChunkDataContainerBase::countByRevision(int key,
                                            const Revision& valueHolder,
                                            const LogicalTime& time) const {
  std::lock_guard<std::mutex> lock(access_mutex_);
  CHECK(isInitialized()) << "Attempted to count items in non-initialized table";
  // Whether valueHolder contains key is implicitly checked whenever using
  // Revision::insertPlaceHolder - for now it's a pretty safe bet that the
  // implementation uses that - this would be rather cumbersome to check here.
  CHECK(time < LogicalTime::sample())
      << "Seeing the future is yet to be implemented ;)";
  return countByRevisionImpl(key, valueHolder, time);
}

void ChunkDataContainerBase::dump(const LogicalTime& time,
                                  ConstRevisionMap* dest) const {
  CHECK_NOTNULL(dest);
  std::shared_ptr<Revision> valueHolder = getTemplate();
  CHECK(valueHolder != nullptr);
  findByRevision(-1, *valueHolder, time, dest);
}

std::ostream& operator<<(std::ostream& stream,
                         const ChunkDataContainerBase::ItemDebugInfo& info) {
  return stream << "For table " << info.table << ", item " << info.id << ": ";
}

bool ChunkDataContainerBase::getLatestUpdateTime(const map_api_common::Id& id,
                                                 LogicalTime* time) {
  CHECK_NE(map_api_common::Id(), id);
  CHECK_NOTNULL(time);
  std::shared_ptr<const Revision> row = getById(id, LogicalTime::sample());
  ItemDebugInfo itemInfo(name(), id);
  if (!row) {
    LOG(ERROR) << itemInfo << "Failed to retrieve row";
    return false;
  }
  *time = row->getUpdateTime();
  return true;
}

} // namespace map_api
