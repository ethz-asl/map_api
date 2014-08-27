#ifndef MAP_API_TRANSACTION_H_
#define MAP_API_TRANSACTION_H_

#include <memory>
#include <map>
#include <string>

#include <glog/logging.h>

#include "map-api/id.h"
#include "map-api/logical-time.h"
#include "map-api/net-table.h"
#include "map-api/net-table-transaction.h"

namespace map_api {
class Chunk;
class ChunkManagerBase;
class NetTableTransaction;
class Revision;

class Transaction {
 public:
  Transaction();
  explicit Transaction(const LogicalTime& begin_time);

  // READ
  /**
   * By Id or chunk:
   * Use the overload with chunk specification to increase performance. Use
   * dumpChunk() for best performance if reading out most of a chunk.
   */
  std::shared_ptr<Revision> getById(const Id& id, NetTable* table);
  std::shared_ptr<Revision> getById(const Id& id, NetTable* table,
                                    Chunk* chunk);
  CRTable::RevisionMap dumpChunk(NetTable* table, Chunk* chunk);
  CRTable::RevisionMap dumpActiveChunks(NetTable* table);
  /**
   * By some other field: Searches in ALL active chunks of a table, thus
   * fundamentally differing from getById or dumpChunk.
   */
  template <typename ValueType>
  CRTable::RevisionMap find(const std::string& key, const ValueType& value,
                            NetTable* table);

  // WRITE
  void insert(
      NetTable* table, Chunk* chunk, std::shared_ptr<Revision> revision);
  /**
   * Uses ChunkManager to auto-size chunks.
   */
  void insert(ChunkManagerBase* chunk_manager,
              std::shared_ptr<Revision> revision);
  void update(NetTable* table, std::shared_ptr<Revision> revision);

  // TRANSACTION OPERATIONS
  bool commit();
  inline LogicalTime getCommitTime() { return commit_time_; }
  using Conflict = ChunkTransaction::Conflict;
  using Conflicts = ChunkTransaction::Conflicts;
  typedef std::unordered_map<NetTable*, ChunkTransaction::Conflicts>
      ConflictMap;
  /**
   * Merge_transaction will be filled with all insertions and non-conflicting
   * updates from this transaction, while the conflicting updates will be
   * represented in a ConflictMap.
   */
  void merge(const std::shared_ptr<Transaction>& merge_transaction,
             ConflictMap* conflicts);
  size_t numChangedItems() const;

 private:
  NetTableTransaction* transactionOf(NetTable* table);

  /**
   * A global ordering of tables prevents deadlocks (resource hierarchy
   * solution)
   */
  struct NetTableOrdering {
    inline bool operator() (const NetTable* a, const NetTable* b) const {
      return CHECK_NOTNULL(a)->name() < CHECK_NOTNULL(b)->name();
    }
  };
  typedef std::map<NetTable*, std::shared_ptr<NetTableTransaction>,
      NetTableOrdering> TransactionMap;
  typedef TransactionMap::value_type TransactionPair;
  TransactionMap net_table_transactions_;
  LogicalTime begin_time_, commit_time_;
};

}  // namespace map_api

#include "map-api/transaction-inl.h"

#endif  // MAP_API_TRANSACTION_H_
