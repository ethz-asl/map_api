#ifndef MAP_API_NET_TABLE_TRANSACTION_H_
#define MAP_API_NET_TABLE_TRANSACTION_H_

#include <memory>
#include <unordered_map>

#include "map-api/chunk.h"
#include "map-api/chunk-transaction.h"
#include "map-api/id.h"
#include "map-api/net-table.h"
#include "map-api/revision.h"
#include "map-api/logical-time.h"

namespace map_api {

class NetTableTransaction {
  friend class Transaction;

 public:
  explicit NetTableTransaction(NetTable* table);
  NetTableTransaction(const LogicalTime& begin_time, NetTable* table);

  /**
   * Equivalent to lock(), if (check()) commit each sub-transaction, unlock()
   * Returns false if check fails.
   */
  bool commit();
  /**
   * Checks all sub-transactions.
   * Returns false if any sub-check fails.
   * lock() MUST have been called
   */
  bool check();
  void insert(Chunk* chunk, std::shared_ptr<Revision> revision);
  /**
   * Locks each chunk affected by this transaction
   */
  void lock();
  void unlock();
  void update(std::shared_ptr<Revision> revision);
  // TODO(tcies) conflict conditions
  std::shared_ptr<Revision> getById(const Id& id);
  // TODO(tcies) all other flavors of reading
  inline LogicalTime time() const {
    return begin_time_;
  }
 private:
  /**
   * Commit with specified time and under the guarantee that the required
   * sub-transactions are locked and checked.
   */
  void checkedCommit(const LogicalTime& time);

  ChunkTransaction* transactionOf(Chunk* chunk);

  /**
   * A global ordering of chunks prevents deadlocks (resource hierarchy
   * solution)
   */
  struct ChunkOrdering {
    inline bool operator() (const Chunk* a, const Chunk* b) {
      return CHECK_NOTNULL(a)->id() < CHECK_NOTNULL(b)->id();
    }
  };
  typedef std::map<Chunk*, std::shared_ptr<ChunkTransaction>, ChunkOrdering>
  TransactionMap;
  typedef TransactionMap::value_type TransactionPair;
  TransactionMap chunk_transactions_;
  LogicalTime begin_time_;
  NetTable* table_;
};

} /* namespace map_api */

#endif /* MAP_API_NET_TABLE_TRANSACTION_H_ */
