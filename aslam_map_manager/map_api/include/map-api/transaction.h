#ifndef MAP_API_TRANSACTION_H_
#define MAP_API_TRANSACTION_H_

#include <memory>
#include <map>

#include <glog/logging.h>

#include "map-api/id.h"
#include "map-api/logical-time.h"
#include "map-api/net-table.h"

namespace map_api {
class Chunk;
class ChunkManagerBase;
class NetTableTransaction;
class Revision;

class Transaction {
 public:
  Transaction();
  explicit Transaction(const LogicalTime& begin_time);
  bool commit();
  std::shared_ptr<Revision> getById(const Id& id, NetTable* table);
  void insert(
      NetTable* table, Chunk* chunk, std::shared_ptr<Revision> revision);
  void insert(ChunkManagerBase* chunk_manager,
              std::shared_ptr<Revision> revision);
  void update(NetTable* table, std::shared_ptr<Revision> revision);

  inline LogicalTime time() const {
    return begin_time_;
  }

 private:
  NetTableTransaction* transactionOf(NetTable* table);

  /**
   * A global ordering of tables prevents deadlocks (resource hierarchy
   * solution)
   */
  struct NetTableOrdering {
    inline bool operator() (const NetTable* a, const NetTable* b) {
      return CHECK_NOTNULL(a)->name() < CHECK_NOTNULL(b)->name();
    }
  };
  typedef std::map<NetTable*, std::shared_ptr<NetTableTransaction>,
      NetTableOrdering> TransactionMap;
  typedef TransactionMap::value_type TransactionPair;
  TransactionMap net_table_transactions_;
  LogicalTime begin_time_;
};

}  // namespace map_api

#endif  // MAP_API_TRANSACTION_H_
