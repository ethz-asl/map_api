#ifndef MAP_API_TRANSACTION_H_
#define MAP_API_TRANSACTION_H_

#include <memory>
#include <unordered_map>

#include "map-api/chunk.h"
#include "map-api/net-table.h"
#include "map-api/net-table-transaction.h"
#include "map-api/revision.h"

namespace map_api {

class Transaction {
 public:
  Transaction();
  explicit Transaction(const Time& begin_time);
  bool commit();
  std::shared_ptr<Revision> getById(NetTable* table, const Id& id);
  void insert(
      NetTable* table, Chunk* chunk, std::shared_ptr<Revision> revision);
  void update(NetTable* table, std::shared_ptr<Revision> revision);
 private:
  NetTableTransaction* transactionOf(NetTable* table);
  typedef std::unordered_map<NetTable*, std::shared_ptr<NetTableTransaction> >
  TransactionMap;
  typedef std::pair<NetTable*, std::shared_ptr<NetTableTransaction> >
  TransactionPair;
  TransactionMap net_table_transactions_;
  Time begin_time_;
};

} /* namespace map_api */

#endif /* MAP_API_TRANSACTION_H_ */
