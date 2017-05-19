#ifndef DMAP_CHUNK_TRANSACTION_H_
#define DMAP_CHUNK_TRANSACTION_H_

#include <list>
#include <memory>
#include <unordered_map>
#include <vector>

#include <gtest/gtest_prod.h>

#include "dmap/internal/chunk-view.h"
#include "dmap/internal/combined-view.h"
#include "dmap/internal/commit-history-view.h"
#include "dmap/internal/delta-view.h"
#include "dmap/logical-time.h"
#include "dmap/net-table.h"
#include "dmap/revision.h"
#include "dmap/revision-map.h"

namespace dmap {
class ChunkBase;
class Conflicts;

namespace internal {
class CommitFuture;
}  // namespace internal

/**
 * This class is somewhat weaker than the first transaction draft
 * (LocalTransaction, now deprecated) because conflict checking and
 * committing is handled in the Chunk class.
 */
class ChunkTransaction {
  friend class internal::CommitFuture;
  friend class NetTableTransaction;
  friend class Transaction;      // for internal typedefs
  friend class NetTableManager;  // metatable works directly with this
  friend class NetTableFixture;
  FRIEND_TEST(ChunkTest, ChunkTransactions);
  FRIEND_TEST(ChunkTest, ChunkTransactionsConflictConditions);

 private:
  ChunkTransaction(ChunkBase* chunk, NetTable* table);
  ChunkTransaction(const LogicalTime& begin_time,
                   const internal::CommitFuture* commit_future,
                   ChunkBase* chunk, NetTable* table);

  // ====
  // READ
  // ====
  template <typename IdType>
  std::shared_ptr<const Revision> getById(const IdType& id) const;
  template <typename ValueType>
  std::shared_ptr<const Revision> findUnique(int key,
                                             const ValueType& value) const;
  void dumpChunk(ConstRevisionMap* result) const;
  template <typename IdType>
  void getAvailableIds(std::unordered_set<IdType>* ids) const;

  // =====
  // WRITE
  // =====
  void insert(std::shared_ptr<Revision> revision);
  void update(std::shared_ptr<Revision> revision);
  // The following function is very dangerous and shouldn't be used apart from
  // where it needs to be used in caches.
  template <typename IdType>
  void getMutableUpdateEntry(const IdType& id,
                             std::shared_ptr<const Revision>** result);
  void remove(std::shared_ptr<Revision> revision);
  template <typename ValueType>
  void addConflictCondition(int key, const ValueType& value);

  // ======================
  // TRANSACTION OPERATIONS
  // ======================
  bool commit();
  bool hasNoConflicts();
  void checkedCommit(const LogicalTime& time);
  /**
   * Merging and changeCount are not compatible with conflict conditions.
   */
  void merge(const std::shared_ptr<ChunkTransaction>& merge_transaction,
             Conflicts* conflicts);
  size_t numChangedItems() const;
  inline void finalize() { finalized_ = true; }
  bool isFinalized() const { return finalized_; }
  void detachFuture();

  // INTERNAL
  typedef std::unordered_multimap<NetTable*, dmap_common::Id> TableToIdMultiMap;
  void getTrackers(const NetTable::NewChunkTrackerMap& overrides,
                   TableToIdMultiMap* trackers) const;

  struct ConflictCondition {
    const int key;
    const std::shared_ptr<Revision> value_holder;
    ConflictCondition(int _key, const std::shared_ptr<Revision>& _value_holder)
        : key(_key), value_holder(_value_holder) {}
  };
  class ConflictVector : public std::vector<ConflictCondition> {};

  ConflictVector conflict_conditions_;

  LogicalTime begin_time_;

  ChunkBase* chunk_;
  NetTable* table_;
  const std::shared_ptr<const Revision> structure_reference_;

  internal::CommitHistoryView::History commit_history_;

  // The combined views are stacked as follows:
  internal::DeltaView delta_;  // Contains uncommitted changes.
  internal::CommitHistoryView commit_history_view_;
  std::unique_ptr<internal::ViewBase> original_view_;

  std::unique_ptr<internal::ViewBase> view_before_delta_;
  internal::CombinedView combined_view_;

  // No more changes will be applied to the data once finalized.
  bool finalized_;
};

}  // namespace dmap

#include "./chunk-transaction-inl.h"

#endif  // DMAP_CHUNK_TRANSACTION_H_
