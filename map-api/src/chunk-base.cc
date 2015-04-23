#include "map-api/chunk-base.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <multiagent-mapping-common/backtrace.h>

DEFINE_bool(blame_trigger, false,
            "Print backtrace for trigger insertion and"
            " invocation.");

namespace map_api {

ChunkBase::~ChunkBase() {}

void ChunkBase::initializeNew(
    const common::Id& id, const std::shared_ptr<TableDescriptor>& descriptor) {
  id_ = id;
  initializeNewImpl(id, descriptor);
  CHECK(data_container_) << "Implementation didn't instantiate data container.";
}

common::Id ChunkBase::id() const { return id_; }

ChunkBase::ConstDataAccess::ConstDataAccess(const ChunkBase& chunk)
    : chunk_(chunk) {
  chunk.readLock();
}

ChunkBase::ConstDataAccess::~ConstDataAccess() { chunk_.unlock(); }

const ChunkDataContainerBase* ChunkBase::ConstDataAccess::operator->() const {
  CHECK(chunk_.data_container_);
  return chunk_.data_container_.get();
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
    LOG(INFO) << "\n" << common::backtrace();
  }
  triggers_.push_back(callback);
  return triggers_.size() - 1u;
}

void ChunkBase::waitForTriggerCompletion() {
  ScopedWriteLock lock(&triggers_are_active_while_has_readers_);
}

void ChunkBase::handleCommitInsert(const common::Id& inserted_id) {
  CHECK(trigger_insertions_.emplace(inserted_id).second);
}

void ChunkBase::handleCommitUpdate(const common::Id& updated_id) {
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
        std::move(std::unordered_set<common::Id>(trigger_insertions_)),
        std::move(std::unordered_set<common::Id>(trigger_updates_)));
    trigger_thread.detach();
  }
  trigger_insertions_.clear();
  trigger_updates_.clear();
}

void ChunkBase::leave() {
  {
    std::unique_lock<std::mutex> lock(trigger_mutex_);
    triggers_.clear();
    // Need to unlock, otherwise we could get into deadlocks, as
    // distributedUnlock() below calls triggers on other peers.
  }
  waitForTriggerCompletion();
  leaveImpl();
}

void ChunkBase::leaveOnceShared() {
  awaitShared();
  leave();
}

void ChunkBase::triggerWrapper(
    const std::unordered_set<common::Id>&& insertions,
    const std::unordered_set<common::Id>&& updates) {
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
