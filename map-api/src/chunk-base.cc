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

#include "map-api/chunk-base.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <map-api-common/backtrace.h>

DEFINE_bool(blame_trigger, false,
            "Print backtrace for trigger insertion and invocation.");

namespace map_api {

ChunkBase::~ChunkBase() {}

void ChunkBase::initializeNew(
    const map_api_common::Id& id, const std::shared_ptr<TableDescriptor>& descriptor) {
  CHECK(descriptor);
  id_ = id;
  initializeNewImpl(id, descriptor);
  CHECK(data_container_) << "Implementation didn't instantiate data container.";
}

map_api_common::Id ChunkBase::id() const { return id_; }

void ChunkBase::getUpdateTimes(
    std::unordered_map<map_api_common::Id, LogicalTime>* result) {
  CHECK_NOTNULL(result)->clear();
  ConstRevisionMap items;
  constData()->dump(LogicalTime::sample(), &items);
  for (const ConstRevisionMap::value_type& id_item : items) {
    result->emplace(id_item.first, id_item.second->getUpdateTime());
  }
}

ChunkBase::ConstDataAccess::ConstDataAccess(const ChunkBase& chunk)
    : chunk_(chunk) {
  chunk.readLock();
}

ChunkBase::ConstDataAccess::~ConstDataAccess() { chunk_.unlock(); }

const ChunkDataContainerBase* ChunkBase::ConstDataAccess::operator->() const {
  return CHECK_NOTNULL(chunk_.data_container_.get());
}

size_t ChunkBase::attachTrigger(const TriggerCallback& callback) {
  std::lock_guard<std::mutex> lock(trigger_mutex_);
  CHECK(callback);
  if (FLAGS_blame_trigger) {
    int status;
    // A yellow line catches the eye better with consecutive attachments.
    LOG(WARNING) << "Trigger of type "
                 << abi::__cxa_demangle(callback.target_type().name(), NULL,
                                        NULL, &status) << " for chunk " << id()
                 << " attached from:";
    LOG(INFO) << map_api_common::backtrace();
  }
  triggers_.push_back(callback);
  return triggers_.size() - 1u;
}

void ChunkBase::waitForTriggerCompletion() {
  map_api_common::ScopedWriteLock lock(&triggers_are_active_while_has_readers_);
}

void ChunkBase::handleCommitInsert(const map_api_common::Id& inserted_id) {
  CHECK(trigger_insertions_.emplace(inserted_id).second);
}

void ChunkBase::handleCommitUpdate(const map_api_common::Id& updated_id) {
  CHECK(trigger_updates_.emplace(updated_id).second);
}

void ChunkBase::handleCommitEnd() {
  std::lock_guard<std::mutex> trigger_lock(trigger_mutex_);
  if (!triggers_.empty()) {
    // Must copy because of
    // http://stackoverflow.com/questions/7895879 .
    // "trigger_insertions_" and "trigger_updates_" are volatile.
    triggers_are_active_while_has_readers_.acquireReadLock();
    std::thread trigger_thread(
        &ChunkBase::triggerWrapper, this,
        std::move(std::unordered_set<map_api_common::Id>(trigger_insertions_)),
        std::move(std::unordered_set<map_api_common::Id>(trigger_updates_)));
    trigger_thread.detach();
  }
  trigger_insertions_.clear();
  trigger_updates_.clear();
}

void ChunkBase::leave() {
  {
    std::unique_lock<std::mutex> lock(trigger_mutex_);
    triggers_.clear();
  }
  waitForTriggerCompletion();
  leaveImpl();
}

void ChunkBase::leaveOnceShared() {
  awaitShared();
  leave();
}

void ChunkBase::triggerWrapper(
    const std::unordered_set<map_api_common::Id>&& insertions,
    const std::unordered_set<map_api_common::Id>&& updates) {
  std::lock_guard<std::mutex> trigger_lock(trigger_mutex_);
  VLOG(3) << triggers_.size() << " triggers called in chunk " << id();
  for (const TriggerCallback& trigger : triggers_) {
    CHECK(trigger);
    trigger(insertions, updates);
  }
  VLOG(3) << "Triggers done.";
  triggers_are_active_while_has_readers_.releaseReadLock();
}

}  // namespace map_api
