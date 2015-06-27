#ifndef MAP_API_NET_TABLE_TRANSACTION_H_
#define MAP_API_NET_TABLE_TRANSACTION_H_

#include <map>
#include <memory>
#include <vector>

#include <gtest/gtest_prod.h>

#include "map-api/chunk-transaction.h"
#include "map-api/logical-time.h"
#include "map-api/net-table.h"
#include "map-api/workspace.h"

namespace map_api {
class ChunkBase;
class ConstRevisionMap;
class Revision;

class NetTableTransaction {
  friend class Transaction;
  friend class NetTableFixture;
  FRIEND_TEST(NetTableFixture, NetTableTransactions);

 private:
  NetTableTransaction(const LogicalTime& begin_time, NetTable* table,
                      const Workspace& workspace);

  // READ (see transaction.h)
  template <typename IdType>
  std::shared_ptr<const Revision> getById(const IdType& id) const;
  template <typename IdType>
  std::shared_ptr<const Revision> getById(const IdType& id,
                                          ChunkBase* chunk) const;
  template <typename IdType>
  std::shared_ptr<const Revision> getByIdFromUncommitted(const IdType& id)
      const;
  void dumpChunk(const ChunkBase* chunk, ConstRevisionMap* result);
  void dumpActiveChunks(ConstRevisionMap* result);
  template <typename ValueType>
  void find(int key, const ValueType& value, ConstRevisionMap* result);
  template <typename IdType>
  void getAvailableIds(std::vector<IdType>* ids);

  // WRITE (see transaction.h)
  void insert(ChunkBase* chunk, std::shared_ptr<Revision> revision);
  void update(std::shared_ptr<Revision> revision);
  void remove(std::shared_ptr<Revision> revision);
  template <typename IdType>
  void remove(const IdType& id);

  // TRANSACTION OPERATIONS
  /**
   * Equivalent to lock(), if (check()) commit each sub-transaction, unlock()
   * Returns false if check fails.
   */
  bool commit();
  /**
   * Commit with specified time and under the guarantee that the required
   * sub-transactions are locked and checked.
   */
  bool checkedCommit(const LogicalTime& time);

  void prepareMultiChunkTransactionInfo(proto::MultiChunkTransactionInfo* info);
  bool sendMultiChunkTransactionInfo(
      const proto::MultiChunkTransactionInfo& info);
  /**
   * Locks each chunk write-affected by this transaction
   */
  void lock();
  void unlock();
  void unlock(bool is_success);
  /**
   * Checks all sub-transactions.
   * Returns false if any sub-check fails.
   * lock() MUST have been called
   */
  bool check();
  void merge(const std::shared_ptr<NetTableTransaction>& merge_transaction,
             ChunkTransaction::Conflicts* conflicts);
  size_t numChangedItems() const;

  // INTERNAL
  ChunkTransaction* transactionOf(const ChunkBase* chunk) const;
  template <typename IdType>
  ChunkBase* chunkOf(const IdType& id,
                     std::shared_ptr<const Revision>* latest) const;

  typedef std::unordered_map<common::Id, ChunkTransaction::TableToIdMultiMap>
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

  NetTable::NewChunkTrackerMap push_new_chunk_ids_to_tracker_overrides_;
};

}  // namespace map_api

#include "./net-table-transaction-inl.h"

#endif  // MAP_API_NET_TABLE_TRANSACTION_H_
