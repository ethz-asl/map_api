#ifndef MAP_API_TRANSACTION_H_
#define MAP_API_TRANSACTION_H_

#include <memory>
#include <map>

#include "map-api/chunk.h"
#include "map-api/net-table.h"
#include "map-api/net-table-transaction.h"
#include "map-api/revision.h"

namespace map_api {

class Transaction {
 public:
  Transaction();
  explicit Transaction(const LogicalTime& begin_time);
  bool commit();
  std::shared_ptr<Revision> getById(const Id& id, NetTable* table);
  template <typename ValueType>
  void find(const std::string& key, const ValueType& value,
            NetTable* table, CRTable::RevisionMap* result);
  void insert(
      NetTable* table, Chunk* chunk, std::shared_ptr<Revision> revision);
  void update(NetTable* table, std::shared_ptr<Revision> revision);

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

} /* namespace map_api */

#include "map-api/transaction-inl.h"

#endif /* MAP_API_TRANSACTION_H_ */
