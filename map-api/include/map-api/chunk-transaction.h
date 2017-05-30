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

#ifndef MAP_API_CHUNK_TRANSACTION_H_
#define MAP_API_CHUNK_TRANSACTION_H_

#include <list>
#include <memory>
#include <unordered_map>
#include <vector>

#include <gtest/gtest_prod.h>

#include "map-api/internal/chunk-view.h"
#include "map-api/internal/combined-view.h"
#include "map-api/internal/commit-history-view.h"
#include "map-api/internal/delta-view.h"
#include "map-api/logical-time.h"
#include "map-api/net-table.h"
#include "map-api/revision.h"
#include "map-api/revision-map.h"

namespace map_api {
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
  typedef std::unordered_multimap<NetTable*, map_api_common::Id> TableToIdMultiMap;
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

}  // namespace map_api

#include "./chunk-transaction-inl.h"

#endif  // MAP_API_CHUNK_TRANSACTION_H_
