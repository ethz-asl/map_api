#include "dmap/chunk-base.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <dmap-common/backtrace.h>

DEFINE_bool(blame_trigger, false,
            "Print backtrace for trigger insertion and invocation.");

namespace dmap {

ChunkBase::~ChunkBase() {}

void ChunkBase::initializeNew(
    const dmap_common::Id& id, const std::shared_ptr<TableDescriptor>& descriptor) {
  CHECK(descriptor);
  id_ = id;
  initializeNewImpl(id, descriptor);
  CHECK(data_container_) << "Implementation didn't instantiate data container.";
}

dmap_common::Id ChunkBase::id() const { return id_; }

void ChunkBase::getUpdateTimes(
    std::unordered_map<dmap_common::Id, LogicalTime>* result) {
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
    LOG(INFO) << dmap_common::backtrace();
  }
  triggers_.push_back(callback);
  return triggers_.size() - 1u;
}

void ChunkBase::waitForTriggerCompletion() {
  dmap_common::ScopedWriteLock lock(&triggers_are_active_while_has_readers_);
}

void ChunkBase::handleCommitInsert(const dmap_common::Id& inserted_id) {
  CHECK(trigger_insertions_.emplace(inserted_id).second);
}

void ChunkBase::handleCommitUpdate(const dmap_common::Id& updated_id) {
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
        std::move(std::unordered_set<dmap_common::Id>(trigger_insertions_)),
        std::move(std::unordered_set<dmap_common::Id>(trigger_updates_)));
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
    const std::unordered_set<dmap_common::Id>&& insertions,
    const std::unordered_set<dmap_common::Id>&& updates) {
  std::lock_guard<std::mutex> trigger_lock(trigger_mutex_);
  VLOG(3) << triggers_.size() << " triggers called in chunk " << id();
  for (const TriggerCallback& trigger : triggers_) {
    CHECK(trigger);
    trigger(insertions, updates);
  }
  VLOG(3) << "Triggers done.";
  triggers_are_active_while_has_readers_.releaseReadLock();
}

}  // namespace dmap
