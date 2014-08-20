#include "map-api/transaction.h"

#include <timing/timer.h>

#include "map-api/chunk.h"
#include "map-api/chunk-manager.h"
#include "map-api/net-table.h"
#include "map-api/net-table-transaction.h"
#include "map-api/revision.h"

namespace map_api {

Transaction::Transaction() : Transaction(LogicalTime::sample()) {}
Transaction::Transaction(const LogicalTime& begin_time)
    : begin_time_(begin_time) {
  CHECK(begin_time < LogicalTime::sample());
}

std::shared_ptr<Revision> Transaction::getById(const Id& id, NetTable* table) {
  CHECK_NOTNULL(table);
  return transactionOf(table)->getById(id);
}

std::shared_ptr<Revision> Transaction::getById(const Id& id, NetTable* table,
                                               Chunk* chunk) {
  CHECK_NOTNULL(table);
  CHECK_NOTNULL(chunk);
  return transactionOf(table)->getById(id, chunk);
}

CRTable::RevisionMap Transaction::dumpChunk(NetTable* table, Chunk* chunk) {
  CHECK_NOTNULL(table);
  CHECK_NOTNULL(chunk);
  return transactionOf(table)->dumpChunk(chunk);
}

CRTable::RevisionMap Transaction::dumpActiveChunks(NetTable* table) {
  CHECK_NOTNULL(table);
  return transactionOf(table)->dumpActiveChunks();
}

void Transaction::insert(
    NetTable* table, Chunk* chunk, std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(table);
  CHECK_NOTNULL(chunk);
  transactionOf(table)->insert(chunk, revision);
}

void Transaction::insert(ChunkManagerBase* chunk_manager,
                         std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(chunk_manager);
  CHECK(revision != nullptr);
  NetTable* table = chunk_manager->getUnderlyingTable();
  CHECK_NOTNULL(table);
  Chunk* chunk = chunk_manager->getChunkForItem(*revision);
  CHECK_NOTNULL(chunk);
  insert(table, chunk, revision);
}

void Transaction::update(NetTable* table, std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(table);
  transactionOf(table)->update(revision);
}

// Deadlocks are prevented by imposing a global ordering on
// net_table_transactions_, and have the locks acquired in that order
// (resource hierarchy solution)
bool Transaction::commit() {
  timing::Timer timer("map_api::Transaction::commit - lock");
  for (const TransactionPair& net_table_transaction : net_table_transactions_) {
    net_table_transaction.second->lock();
  }
  timer.Stop();
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
