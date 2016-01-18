#include <map-api/legacy-chunk.h>
#include "map-api/transaction.h"

#include <algorithm>

#include <multiagent-mapping-common/backtrace.h>
#include <timing/timer.h>

#include "map-api/cache-base.h"
#include "map-api/chunk-manager.h"
#include "map-api/conflicts.h"
#include "map-api/internal/commit-future.h"
#include "map-api/net-table.h"
#include "map-api/net-table-manager.h"
#include "map-api/net-table-transaction.h"
#include "map-api/revision.h"
#include "map-api/trackee-multimap.h"
#include "map-api/workspace.h"
#include "./core.pb.h"

DECLARE_bool(cache_blame_dirty);
DECLARE_bool(cache_blame_insert);
DEFINE_bool(blame_commit, false, "Print stack trace for every commit");

namespace map_api {

Transaction::Transaction(const std::shared_ptr<Workspace>& workspace,
                         const LogicalTime& begin_time,
                         const CommitFutureTree* commit_futures)
    : workspace_(workspace),
      begin_time_(begin_time),
      chunk_tracking_disabled_(false),
      is_parallel_commit_running_(false),
      finalized_(false) {
  CHECK(begin_time < LogicalTime::sample());
  if (commit_futures != nullptr) {
    for (const CommitFutureTree::value_type& table_commit_futures :
         *commit_futures) {
      net_table_transactions_[table_commit_futures.first] =
          std::shared_ptr<NetTableTransaction>(new NetTableTransaction(
              begin_time, *workspace, &table_commit_futures.second,
              table_commit_futures.first));
    }
  }
}

Transaction::Transaction(const std::shared_ptr<Workspace>& workspace,
                         const LogicalTime& begin_time)
    : Transaction(workspace, begin_time, nullptr) {}
Transaction::Transaction()
    : Transaction(std::shared_ptr<Workspace>(new Workspace),
                  LogicalTime::sample()) {}
Transaction::Transaction(const std::shared_ptr<Workspace>& workspace)
    : Transaction(workspace, LogicalTime::sample()) {}
Transaction::Transaction(const LogicalTime& begin_time)
    : Transaction(std::shared_ptr<Workspace>(new Workspace), begin_time) {}
Transaction::Transaction(const CommitFutureTree& commit_futures)
    : Transaction(std::shared_ptr<Workspace>(new Workspace),
                  LogicalTime::sample(), &commit_futures) {}

Transaction::~Transaction() { joinParallelCommitIfRunning(); }

void Transaction::dumpChunk(NetTable* table, ChunkBase* chunk,
                            ConstRevisionMap* result) {
  CHECK_NOTNULL(table);
  CHECK_NOTNULL(chunk);
  CHECK_NOTNULL(result);
  if (!workspace_->contains(table, chunk->id())) {
    result->clear();
  } else {
    transactionOf(table)->dumpChunk(chunk, result);
  }
}

void Transaction::dumpActiveChunks(NetTable* table, ConstRevisionMap* result) {
  CHECK_NOTNULL(table);
  CHECK_NOTNULL(result);
  if (!workspace_->contains(table)) {
    result->clear();
  } else {
    transactionOf(table)->dumpActiveChunks(result);
  }
}

bool Transaction::fetchAllChunksTrackedByItemsInTable(NetTable* const table) {
  CHECK_NOTNULL(table);
  std::vector<common::Id> item_ids;
  enableDirectAccess();
  getAvailableIds(table, &item_ids);

  bool success = true;
  for (const common::Id& item_id : item_ids) {
    if (!getById(item_id, table)->fetchTrackedChunks()) {
      success = false;
    }
  }
  disableDirectAccess();
  refreshIdToChunkIdMaps();
  // Id to chunk id maps must be refreshed first, otherwise getAvailableIds will
  // nor work.
  refreshAvailableIdsInCaches();
  return success;
}

void Transaction::insert(NetTable* table, ChunkBase* chunk,
                         std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(table);
  CHECK_NOTNULL(chunk);
  CHECK(!finalized_);
  transactionOf(table)->insert(chunk, revision);
}

void Transaction::insert(ChunkManagerBase* chunk_manager,
                         std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(chunk_manager);
  CHECK(revision != nullptr);
  CHECK(!finalized_);
  NetTable* table = chunk_manager->getUnderlyingTable();
  CHECK_NOTNULL(table);
  ChunkBase* chunk = chunk_manager->getChunkForItem(*revision);
  CHECK_NOTNULL(chunk);
  insert(table, chunk, revision);
}

void Transaction::update(NetTable* table, std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(table);
  CHECK(!finalized_);
  transactionOf(table)->update(revision);
}

void Transaction::remove(NetTable* table, std::shared_ptr<Revision> revision) {
  CHECK(!finalized_);
  transactionOf(CHECK_NOTNULL(table))->remove(revision);
}

// Deadlocks are prevented by imposing a global ordering on
// net_table_transactions_, and have the locks acquired in that order
// (resource hierarchy solution)
bool Transaction::commit() {
  std::promise<bool> will_commit_succeed;
  commitImpl(false, &will_commit_succeed);
  return will_commit_succeed.get_future().get();
}

bool Transaction::commitInParallel(CommitFutureTree* future_tree) {
  CHECK_NOTNULL(future_tree);
  std::promise<bool> will_commit_succeed;
  std::thread([this, &will_commit_succeed]() {
                {
                  std::lock_guard<std::mutex> lock(
                      m_is_parallel_commit_running_);
                  CHECK(!is_parallel_commit_running_);
                  is_parallel_commit_running_ = true;
                  cv_is_parallel_commit_running_.notify_all();
    }
    commitImpl(true, &will_commit_succeed);
    CHECK(finalized_);
    {
      std::lock_guard<std::mutex> lock(m_is_parallel_commit_running_);
      is_parallel_commit_running_ = false;
      cv_is_parallel_commit_running_.notify_all();
    }
              }).detach();
  if (will_commit_succeed.get_future().get()) {
    for (const TransactionPair& table_transaction : net_table_transactions_) {
      NetTableTransaction::CommitFutureTree& subtree =
          (*future_tree)[table_transaction.first];
      table_transaction.second->buildCommitFutureTree(&subtree);
    }
    return true;
  } else {
    return false;
  }
}

void Transaction::joinParallelCommitIfRunning() {
  std::unique_lock<std::mutex> lock(m_is_parallel_commit_running_);
  cv_is_parallel_commit_running_.wait(
      lock, [this] { return !is_parallel_commit_running_; });
}

void Transaction::merge(const std::shared_ptr<Transaction>& merge_transaction,
                        ConflictMap* conflicts) {
  CHECK(merge_transaction.get() != nullptr) << "Merge requires an initiated "
                                               "transaction";
  CHECK_NOTNULL(conflicts);
  conflicts->clear();
  for (const TransactionPair& net_table_transaction : net_table_transactions_) {
    std::shared_ptr<NetTableTransaction> merge_net_table_transaction(
        new NetTableTransaction(merge_transaction->begin_time_, *workspace_,
                                nullptr, net_table_transaction.first));
    Conflicts sub_conflicts;
    net_table_transaction.second->merge(merge_net_table_transaction,
                                        &sub_conflicts);
    CHECK_EQ(
        net_table_transaction.second->numChangedItems(),
        merge_net_table_transaction->numChangedItems() + sub_conflicts.size());
    if (merge_net_table_transaction->numChangedItems() > 0u) {
      merge_transaction->net_table_transactions_.insert(std::make_pair(
          net_table_transaction.first, merge_net_table_transaction));
    }
    if (!sub_conflicts.empty()) {
      std::pair<ConflictMap::iterator, bool> insert_result = conflicts->insert(
          std::make_pair(net_table_transaction.first, Conflicts()));
      CHECK(insert_result.second);
      insert_result.first->second.swap(sub_conflicts);
    }
  }
}

void Transaction::detachFutures() {
  for (TransactionPair& table_transaction : net_table_transactions_) {
    table_transaction.second->detachFutures();
  }
}

size_t Transaction::numChangedItems() const {
  size_t count = 0u;
  for (const TransactionPair& net_table_transaction : net_table_transactions_) {
    count += net_table_transaction.second->numChangedItems();
  }
  return count;
}

void Transaction::refreshIdToChunkIdMaps() {
  CHECK(!finalized_);
  for (TransactionPair& net_table_transaction : net_table_transactions_) {
    net_table_transaction.second->refreshIdToChunkIdMap();
  }
}

void Transaction::refreshAvailableIdsInCaches() {
  CHECK(!finalized_);
  for (const CacheMap::value_type& cache_pair : caches_) {
    cache_pair.second->refreshAvailableIds();
  }
}

void Transaction::enableDirectAccess() {
  std::lock_guard<std::mutex> lock(access_type_mutex_);
  CHECK(cache_access_override_.insert(std::this_thread::get_id()).second);
}

void Transaction::disableDirectAccess() {
  std::lock_guard<std::mutex> lock(access_type_mutex_);
  CHECK_EQ(1u, cache_access_override_.erase(std::this_thread::get_id()));
}

NetTableTransaction* Transaction::transactionOf(NetTable* table) const {
  CHECK_NOTNULL(table);
  ensureAccessIsDirect(table);
  std::lock_guard<std::mutex> lock(net_table_transactions_mutex_);
  TransactionMap::const_iterator net_table_transaction =
      net_table_transactions_.find(table);
  if (net_table_transaction == net_table_transactions_.end()) {
    CHECK(!finalized_);
    std::shared_ptr<NetTableTransaction> transaction(
        new NetTableTransaction(begin_time_, *workspace_, nullptr, table));
    std::pair<TransactionMap::iterator, bool> inserted =
        net_table_transactions_.insert(std::make_pair(table, transaction));
    CHECK(inserted.second);
    net_table_transaction = inserted.first;
  }
  return net_table_transaction->second.get();
}

void Transaction::ensureAccessIsCache(NetTable* table) const {
  std::lock_guard<std::mutex> lock(access_mode_mutex_);
  TableAccessModeMap::iterator found = access_mode_.find(table);
  if (found == access_mode_.end()) {
    access_mode_[table] = TableAccessMode::kCache;
  } else {
    CHECK(found->second == TableAccessMode::kCache)
        << "Access mode for table " << table->name() << " is already direct, "
                                                        "may not attach cache.";
  }
}

void Transaction::ensureAccessIsDirect(NetTable* table) const {
  std::unique_lock<std::mutex> lock(access_mode_mutex_);
  TableAccessModeMap::iterator found = access_mode_.find(table);
  if (found == access_mode_.end()) {
    access_mode_[table] = TableAccessMode::kDirect;
  } else {
    if (found->second != TableAccessMode::kDirect) {
      lock.unlock();
      std::lock_guard<std::mutex> lock(access_type_mutex_);
      CHECK(cache_access_override_.find(std::this_thread::get_id()) !=
            cache_access_override_.end())
          << "Access mode for table " << table->name()
          << " is already by cache, may not access directly.";
    }
  }
}

void Transaction::pushNewChunkIdsToTrackers() {
  CHECK(!finalized_);
  if (chunk_tracking_disabled_) {
    return;
  }
  // tracked table -> tracked chunks -> tracking table -> tracking item
  typedef std::unordered_map<NetTable*,
                             NetTableTransaction::TrackedChunkToTrackersMap>
      TrackeeToTrackerMap;
  TrackeeToTrackerMap net_table_chunk_trackers;
  for (const TransactionMap::value_type& table_transaction :
       net_table_transactions_) {
    table_transaction.second->getChunkTrackers(
        &net_table_chunk_trackers[table_transaction.first]);
  }
  // tracking item -> tracked table -> tracked chunks
  typedef std::unordered_map<common::Id, TrackeeMultimap> ItemToTrackeeMap;
  // tracking table -> tracking item -> tracked table -> tracked chunks
  typedef std::unordered_map<NetTable*, ItemToTrackeeMap> TrackerToTrackeeMap;
  TrackerToTrackeeMap table_item_chunks_to_push;
  for (const TrackeeToTrackerMap::value_type& net_table_trackers :
       net_table_chunk_trackers) {
    for (const NetTableTransaction::TrackedChunkToTrackersMap::value_type&
             chunk_trackers : net_table_trackers.second) {
      for (const ChunkTransaction::TableToIdMultiMap::value_type& tracker :
           chunk_trackers.second) {
        table_item_chunks_to_push[tracker.first][tracker.second]
                                 [net_table_trackers.first]
                                     .emplace(chunk_trackers.first);
      }
    }
  }

  for (const TrackerToTrackeeMap::value_type& table_chunks_to_push :
       table_item_chunks_to_push) {
    for (const ItemToTrackeeMap::value_type& item_chunks_to_push :
         table_chunks_to_push.second) {
      CHECK(item_chunks_to_push.first.isValid())
          << "Invalid tracker ID for trackee from "
          << "table " << table_chunks_to_push.first->name();
      std::shared_ptr<const Revision> original_tracker =
          getById(item_chunks_to_push.first, table_chunks_to_push.first);

      TrackeeMultimap trackee_multimap;
      trackee_multimap.deserialize(*original_tracker->underlying_revision_);
      // Update only if set of trackees has changed.
      if (trackee_multimap.merge(item_chunks_to_push.second)) {
        std::shared_ptr<Revision> updated_tracker;
        original_tracker->copyForWrite(&updated_tracker);
        trackee_multimap.serialize(updated_tracker->underlying_revision_.get());
        update(table_chunks_to_push.first, updated_tracker);
      }
    }
  }
}

void Transaction::commitImpl(const bool finalize_after_check,
                             std::promise<bool>* will_commit_succeed) {
  CHECK_NOTNULL(will_commit_succeed);
  if (FLAGS_blame_commit) {
    LOG(INFO) << "Transaction committed from:" << std::endl
              << common::backtrace();
  }
  for (const CacheMap::value_type& cache_pair : caches_) {
    if (FLAGS_cache_blame_dirty || FLAGS_cache_blame_insert) {
      std::cout << cache_pair.first->name() << " cache:" << std::endl;
    }
    cache_pair.second->prepareForCommit();
  }
  enableDirectAccess();
  pushNewChunkIdsToTrackers();
  disableDirectAccess();

  // This must happen after chunk tracker resolution, since chunk tracker
  // resolution might access the cache in read-mode, but we won't be able to
  // fetch the proper metadata until after the commit!
  for (const CacheMap::value_type& cache_pair : caches_) {
    cache_pair.second->discardCachedInsertions();
  }
  timing::Timer timer("map_api::Transaction::commit - lock");
  for (const TransactionPair& net_table_transaction : net_table_transactions_) {
    net_table_transaction.second->lock();
  }
  timer.Stop();
  for (const TransactionPair& net_table_transaction : net_table_transactions_) {
    if (!net_table_transaction.second->hasNoConflicts()) {
      will_commit_succeed->set_value(false);
      for (const TransactionPair& net_table_transaction :
           net_table_transactions_) {
        net_table_transaction.second->unlock();
      }
      return;
    }
  }

  if (finalize_after_check) {
    finalize();
  }

  commit_time_ = LogicalTime::sample();
  // Promise must happen after setting commit_time_, since the begin time of the
  // subsequent transaction must be after the commit time.
  will_commit_succeed->set_value(true);
  VLOG(4) << "Commit from " << begin_time_ << " to " << commit_time_;
  for (const TransactionPair& net_table_transaction : net_table_transactions_) {
    net_table_transaction.second->checkedCommit(commit_time_);
    net_table_transaction.second->unlock();
  }
}

void Transaction::finalize() {
  finalized_ = true;
  for (const TransactionPair& table_transaction : net_table_transactions_) {
    table_transaction.second->finalize();
  }
}

}  // namespace map_api */
