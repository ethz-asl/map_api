#include "map-api/net-table-transaction.h"

namespace map_api {

NetTableTransaction::NetTableTransaction(NetTable* table)
: NetTableTransaction(LogicalTime::sample(), table) {}

NetTableTransaction::NetTableTransaction(
    const LogicalTime& begin_time, NetTable* table) : begin_time_(begin_time),
        table_(table) {
  CHECK(begin_time <= LogicalTime::sample());
}

bool NetTableTransaction::check() {
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    if (!chunk_transaction.first->check(*chunk_transaction.second)) {
      return false;
    }
  }
  return true;
}

// Deadlocks in lock() are prevented by imposing a global ordering on chunks,
// and have the locks acquired in that order (resource hierarchy solution)
bool NetTableTransaction::commit() {
  lock();
  if (!check()) {
    unlock();
    return false;
  }
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    CHECK(chunk_transaction.first->commit(*chunk_transaction.second));
  }
  unlock();
  return true;
}

void NetTableTransaction::insert(
    Chunk* chunk, std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(chunk);
  transactionOf(chunk)->insert(revision);
}

// Deadlocks in lock() are prevented by imposing a global ordering on chunks,
// and have the locks acquired in that order (resource hierarchy solution)
void NetTableTransaction::lock() {
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    chunk_transaction.first->lock();
  }
}

void NetTableTransaction::unlock() {
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    chunk_transaction.first->unlock();
  }
}

void NetTableTransaction::update(std::shared_ptr<Revision> revision) {
  Id chunk_id;
  revision->get(NetTable::kChunkIdField, &chunk_id);
  Chunk* chunk = table_->getChunk(chunk_id);
  transactionOf(chunk)->update(revision);
}

std::shared_ptr<Revision> NetTableTransaction::getById(const Id& id) {
  std::shared_ptr<Revision> uncommitted;
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    uncommitted = chunk_transaction.second->getByIdFromUncommitted(id);
    if (uncommitted) {
      return uncommitted;
    }
  }
  return table_->getById(id, begin_time_);
}

ChunkTransaction* NetTableTransaction::transactionOf(Chunk* chunk) {
  CHECK_NOTNULL(chunk);
  TransactionMap::iterator chunk_transaction = chunk_transactions_.find(chunk);
  if (chunk_transaction == chunk_transactions_.end()) {
    std::shared_ptr<ChunkTransaction> transaction =
        chunk->newTransaction(begin_time_);
    std::pair<TransactionMap::iterator, bool> inserted =
        chunk_transactions_.insert(std::make_pair(chunk, transaction));
    CHECK(inserted.second);
    chunk_transaction = inserted.first;
  }
  return chunk_transaction->second.get();
}

} /* namespace map_api */
