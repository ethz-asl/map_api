#include "map-api/chunk-transaction.h"

#include <unordered_set>

#include <multiagent-mapping-common/accessors.h>

#include "map-api/conflicts.h"
#include "map-api/internal/commit-future.h"
#include "map-api/net-table.h"

namespace map_api {

ChunkTransaction::ChunkTransaction(ChunkBase* chunk, NetTable* table)
    : ChunkTransaction(LogicalTime::sample(), nullptr, chunk, table) {}

ChunkTransaction::ChunkTransaction(const LogicalTime& begin_time,
                                   const internal::CommitFuture* commit_future,
                                   ChunkBase* chunk, NetTable* table)
    : begin_time_(begin_time),
      chunk_(CHECK_NOTNULL(chunk)),
      table_(CHECK_NOTNULL(table)),
      structure_reference_(chunk_->constData()->getTemplate()),
      delta_(*table),
      commit_history_view_(commit_history_, *chunk),
      original_view_(commit_future
                         ? static_cast<internal::ViewBase*>(
                               new internal::CommitFuture(*commit_future))
                         : static_cast<internal::ViewBase*>(
                               new internal::ChunkView(*chunk, begin_time))),
      view_before_delta_(
          new internal::CombinedView(original_view_, commit_history_view_)),
      combined_view_(view_before_delta_, delta_),
      finalized_(false) {
  CHECK(begin_time < LogicalTime::sample());
}

void ChunkTransaction::dumpChunk(ConstRevisionMap* result) const {
  CHECK_NOTNULL(result);
  combined_view_.dump(result);
}

void ChunkTransaction::insert(std::shared_ptr<Revision> revision) {
  CHECK(!finalized_);
  CHECK_NOTNULL(revision.get());
  CHECK(revision->structureMatch(*structure_reference_));
  delta_.insert(revision);
}

void ChunkTransaction::update(std::shared_ptr<Revision> revision) {
  CHECK(!finalized_);
  CHECK_NOTNULL(revision.get());
  CHECK(revision->structureMatch(*structure_reference_));
  delta_.update(revision);
}

void ChunkTransaction::remove(std::shared_ptr<Revision> revision) {
  CHECK(!finalized_);
  CHECK_NOTNULL(revision.get());
  CHECK(revision->structureMatch(*structure_reference_));
  delta_.remove(revision);
}

bool ChunkTransaction::commit() {
  chunk_->writeLock();
  if (!check()) {
    chunk_->unlock();
    return false;
  }
  checkedCommit(LogicalTime::sample());
  chunk_->unlock();
  return true;
}

bool ChunkTransaction::check() {
  CHECK(!finalized_);  // Because checking can try to auto-merge.
  CHECK(chunk_->isWriteLocked());
  std::unordered_map<common::Id, LogicalTime> update_times;
  chunk_->getUpdateTimes(&update_times);
  view_before_delta_->discardKnownUpdates(&update_times);

  internal::ChunkView current_view_(*chunk_, LogicalTime::sample());
  if (delta_.hasConflictsAfterTryingToMerge(update_times, *view_before_delta_,
                                            current_view_)) {
    return false;
  }

  // TODO(tcies) embed in view concept?
  for (const ChunkTransaction::ConflictCondition& item : conflict_conditions_) {
    ConstRevisionMap dummy;
    chunk_->data_container_->findByRevision(item.key, *item.value_holder,
                                            LogicalTime::sample(), &dummy);
    if (!dummy.empty()) {
      VLOG(4) << "Conflict condition in table " << table_->name();
      return false;
    }
  }
  return true;
}

void ChunkTransaction::checkedCommit(const LogicalTime& time) {
  delta_.checkedCommitLocked(time, chunk_, &commit_history_);
}

void ChunkTransaction::merge(
    const std::shared_ptr<ChunkTransaction>& merge_transaction,
    Conflicts* conflicts) {
  CHECK_NOTNULL(merge_transaction.get());
  CHECK_NOTNULL(conflicts);
  CHECK(conflict_conditions_.empty()) << "merge not compatible with conflict "
                                         "conditions";

  chunk_->readLock();
  std::unordered_map<common::Id, LogicalTime> update_times;
  chunk_->getUpdateTimes(&update_times);
  view_before_delta_->discardKnownUpdates(&update_times);

  internal::ChunkView current_view_(*chunk_, LogicalTime::sample());
  delta_.prepareManualMerge(update_times, *view_before_delta_, current_view_,
                            &merge_transaction->delta_, conflicts);
  chunk_->unlock();
}

size_t ChunkTransaction::numChangedItems() const {
  CHECK(conflict_conditions_.empty()) << "changeCount not compatible with "
                                         "conflict conditions";
  return delta_.numChanges();
}

void ChunkTransaction::detachFuture() {
  original_view_.reset(new internal::ChunkView(*chunk_, begin_time_));
}

void ChunkTransaction::getTrackers(
    const NetTable::NewChunkTrackerMap& overrides,
    TableToIdMultiMap* trackers) const {
  CHECK_NOTNULL(trackers);
  for (const typename NetTable::NewChunkTrackerMap::value_type&
           table_tracker_getter : table_->new_chunk_trackers()) {
    NetTable::NewChunkTrackerMap::const_iterator override_it =
        overrides.find(table_tracker_getter.first);
    const std::function<common::Id(const Revision&)>& tracker_id_extractor =
        ((override_it != overrides.end()) ? (override_it->second)
                                          : (table_tracker_getter.second));
    // TODO(tcies) add function to delta.
    for (const internal::DeltaView::InsertMap::value_type& insertion :
         delta_.insertions_) {
      common::Id id = tracker_id_extractor(*insertion.second);
      trackers->emplace(table_tracker_getter.first, id);
    }
  }
}

}  // namespace map_api
