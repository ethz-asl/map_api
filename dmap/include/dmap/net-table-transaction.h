#ifndef DMAP_NET_TABLE_TRANSACTION_H_
#define DMAP_NET_TABLE_TRANSACTION_H_

#include <map>
#include <memory>
#include <vector>

#include <gtest/gtest_prod.h>

#include "dmap/chunk-transaction.h"
#include "dmap/logical-time.h"
#include "dmap/net-table.h"
#include "dmap/workspace.h"

namespace dmap {
class ChunkBase;
class Conflicts;
class ConstRevisionMap;
class Revision;

namespace internal {
class CommitFuture;
}  // namespace internal

class NetTableTransaction {
  friend class Transaction;
  friend class NetTableFixture;
  FRIEND_TEST(NetTableTest, NetTableTransactions);

 private:
  typedef std::unordered_map<
      ChunkBase*, std::unique_ptr<internal::CommitFuture>> CommitFutureTree;

  NetTableTransaction(const LogicalTime& begin_time, const Workspace& workspace,
                      const CommitFutureTree* commit_futures, NetTable* table);

  // ========================
  // READ (see transaction.h)
  // ========================
  template <typename IdType>
  std::shared_ptr<const Revision> getById(const IdType& id) const;
  // This will be minimally faster.
  template <typename IdType>
  std::shared_ptr<const Revision> getById(const IdType& id,
                                          ChunkBase* chunk) const;
  void dumpChunk(const ChunkBase* chunk, ConstRevisionMap* result);
  void dumpActiveChunks(ConstRevisionMap* result);
  template <typename ValueType>
  void find(int key, const ValueType& value, ConstRevisionMap* result);
  template <typename IdType>
  void getAvailableIds(std::vector<IdType>* ids);

  // =========================
  // WRITE (see transaction.h)
  // =========================
  void insert(ChunkBase* chunk, std::shared_ptr<Revision> revision);
  void update(std::shared_ptr<Revision> revision);
  // The following function is very dangerous and shouldn't be used apart from
  // where it needs to be used in caches.
  template <typename IdType>
  std::shared_ptr<const Revision>* getMutableUpdateEntry(const IdType& id);
  void remove(std::shared_ptr<Revision> revision);
  template <typename IdType>
  void remove(const IdType& id);

  // ======================
  // TRANSACTION OPERATIONS
  // ======================
  /**
   * Equivalent to lock(), if (check()) commit each sub-transaction, unlock()
   * Returns false if check fails.
   */
  bool commit();
  /**
   * Commit with specified time and under the guarantee that the required
   * sub-transactions are locked and checked.
   */
  void checkedCommit(const LogicalTime& time);
  /**
   * Locks each chunk write-affected by this transaction
   */
  void lock();
  void unlock();

  bool hasNoConflicts();
  void merge(const std::shared_ptr<NetTableTransaction>& merge_transaction,
             Conflicts* conflicts);
  size_t numChangedItems() const;

  void finalize();
  void buildCommitFutureTree(CommitFutureTree* result);
  void detachFutures();

  // ========
  // INTERNAL
  // ========
  ChunkTransaction* transactionOf(const ChunkBase* chunk) const;
  template <typename IdType>
  ChunkBase* chunkOf(const IdType& id) const;
  // The following must be called if chunks are fetched after the transaction
  // has been initialized, otherwise the new items can't be fetched by the
  // transaction.
  void refreshIdToChunkIdMap();

  typedef std::unordered_map<dmap_common::Id, ChunkTransaction::TableToIdMultiMap>
      TrackedChunkToTrackersMap;
  void getChunkTrackers(TrackedChunkToTrackersMap* chunk_trackers) const;

  template <typename TrackerIdType>
  void overrideTrackerIdentificationMethod(
      NetTable* tracker_table,
      const std::function<TrackerIdType(const Revision&)>&
          how_to_determine_tracker);

  /**
   * A global ordering of chunks prevents deadlocks (resource hierarchy
   * solution)
   */
  struct ChunkOrdering {
    inline bool operator()(const ChunkBase* a, const ChunkBase* b) const {
      return CHECK_NOTNULL(a)->id() < CHECK_NOTNULL(b)->id();
    }
  };

  typedef std::map<ChunkBase*, std::shared_ptr<ChunkTransaction>, ChunkOrdering>
      TransactionMap;
  typedef TransactionMap::value_type TransactionPair;
  mutable TransactionMap chunk_transactions_;
  LogicalTime begin_time_;
  NetTable* table_;
  Workspace::TableInterface workspace_;

  typedef std::unordered_map<dmap_common::Id, dmap_common::Id> ItemIdToChunkIdMap;
  ItemIdToChunkIdMap item_id_to_chunk_id_map_;

  NetTable::NewChunkTrackerMap push_new_chunk_ids_to_tracker_overrides_;

  bool finalized_;
};

}  // namespace dmap

#include "./net-table-transaction-inl.h"

#endif  // DMAP_NET_TABLE_TRANSACTION_H_
