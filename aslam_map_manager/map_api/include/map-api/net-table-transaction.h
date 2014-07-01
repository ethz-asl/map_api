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
 public:
  explicit NetTableTransaction(NetTable* table);
  NetTableTransaction(const LogicalTime& begin_time, NetTable* table);

  /**
   * Checks all sub-transactions.
   * Returns false if any sub-check fails.
   * lock() MUST have been called
   */
  bool check();
  /**
   * Equivalent to lock(), if (check()) commit each sub-transaction, unlock()
   * Returns false if check fails.
   */
  bool commit();
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
 private:
  ChunkTransaction* transactionOf(Chunk* chunk);

  // Id is id of chunk TODO(tcies) strong typing?
  typedef std::unordered_map<Chunk*, std::shared_ptr<ChunkTransaction> >
  TransactionMap;
  typedef std::pair<Chunk*, std::shared_ptr<ChunkTransaction> >
  TransactionPair;
  TransactionMap chunk_transactions_;
  LogicalTime begin_time_;
  NetTable* table_;
};

} /* namespace map_api */

#endif /* MAP_API_NET_TABLE_TRANSACTION_H_ */
