#include "map-api/transaction.h"

namespace map_api {

Transaction::Transaction() : Transaction(LogicalTime::sample()) {}
Transaction::Transaction(const LogicalTime& begin_time) : begin_time_(begin_time) {
  CHECK(begin_time < LogicalTime::sample());
}

// Deadlocks are prevented by imposing a global ordering on
// net_table_transactions_, and have the locks acquired in that order
// (resource hierarchy solution)
bool Transaction::commit() {
  for (const TransactionPair& net_table_transaction : net_table_transactions_) {
    net_table_transaction.second->lock();
  }
  for (const TransactionPair& net_table_transaction : net_table_transactions_) {
    if (!net_table_transaction.second->check()) {
      for (const TransactionPair& net_table_transaction :
          net_table_transactions_) {
        net_table_transaction.second->unlock();
      }
      return false;
    }
  }
  LogicalTime commit_time_ = LogicalTime::sample();
  for (const TransactionPair& net_table_transaction : net_table_transactions_) {
    net_table_transaction.second->checkedCommit(commit_time_);
    net_table_transaction.second->unlock();
  }
  return true;
}

std::shared_ptr<Revision> Transaction::getById(const Id& id, NetTable* table) {
  CHECK_NOTNULL(table);
  return transactionOf(table)->getById(id);
}

void Transaction::insert(
    NetTable* table, Chunk* chunk, std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(table);
  CHECK_NOTNULL(chunk);
  transactionOf(table)->insert(chunk, revision);
}

void Transaction::update(NetTable* table, std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(table);
  transactionOf(table)->update(revision);
}

NetTableTransaction* Transaction::transactionOf(NetTable* table) {
  CHECK_NOTNULL(table);
  TransactionMap::iterator net_table_transaction =
      net_table_transactions_.find(table);
  if (net_table_transaction == net_table_transactions_.end()) {
    std::shared_ptr<NetTableTransaction> transaction(
        new NetTableTransaction(begin_time_, table));
    std::pair<TransactionMap::iterator, bool> inserted =
        net_table_transactions_.insert(std::make_pair(table, transaction));
    CHECK(inserted.second);
    net_table_transaction = inserted.first;
  }
  return net_table_transaction->second.get();
}

} /* namespace map_api */
