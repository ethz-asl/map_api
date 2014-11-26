#include <map-api/net-table-transaction.h>
#include <statistics/statistics.h>

namespace map_api {

NetTableTransaction::NetTableTransaction(NetTable* table)
: NetTableTransaction(LogicalTime::sample(), table) {}

NetTableTransaction::NetTableTransaction(
    const LogicalTime& begin_time, NetTable* table) : begin_time_(begin_time),
        table_(table) {
  CHECK(begin_time < LogicalTime::sample());
}

CRTable::RevisionMap NetTableTransaction::dumpChunk(Chunk* chunk) {
  CHECK_NOTNULL(chunk);
  return transactionOf(chunk)->dumpChunk();
}

CRTable::RevisionMap NetTableTransaction::dumpActiveChunks() {
  CRTable::RevisionMap result;
  table_->dumpActiveChunks(begin_time_, &result);
  return result;
}

void NetTableTransaction::insert(Chunk* chunk,
                                 std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(chunk);
  transactionOf(chunk)->insert(revision);
}

void NetTableTransaction::update(std::shared_ptr<Revision> revision) {
  Id id = revision->getId<Id>();
  CHECK(id.isValid());
  if (!revision->getChunkId().isValid()) {
    // Can be the case if an uncommitted revision is being updated.
    for (TransactionMap::value_type& chunk_transaction : chunk_transactions_) {
      if (chunk_transaction.second->getByIdFromUncommitted(id)) {
        chunk_transaction.second->update(revision);
        return;
      }
    }
    LOG(FATAL) << "Chunk id of revision to update invalid, yet revision " << id
               << " can't be found among uncommitted.";
  }
  Chunk* chunk = table_->getChunk(revision->getChunkId());
  transactionOf(chunk)->update(revision);
}

void NetTableTransaction::remove(std::shared_ptr<Revision> revision) {
  Chunk* chunk = table_->getChunk(revision->getChunkId());
  transactionOf(chunk)->remove(revision);
}

bool NetTableTransaction::commit() {
  lock();
  if (!check()) {
    unlock();
    return false;
  }
  checkedCommit(LogicalTime::sample());
  unlock();
  return true;
}

void NetTableTransaction::checkedCommit(const LogicalTime& time) {
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    chunk_transaction.second->checkedCommit(time);
  }
}

// Deadlocks in lock() are prevented by imposing a global ordering on chunks,
// and have the locks acquired in that order (resource hierarchy solution)
void NetTableTransaction::lock() {
  size_t i = 0u;
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    chunk_transaction.first->writeLock();
    ++i;
  }
  statistics::StatsCollector stat("map_api::NetTableTransaction::lock - " +
                                  table_->name());
  stat.AddSample(i);
}

void NetTableTransaction::unlock() {
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    chunk_transaction.first->unlock();
  }
}

bool NetTableTransaction::check() {
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    if (!chunk_transaction.second->check()) {
      return false;
    }
  }
  return true;
}

void NetTableTransaction::merge(
    const std::shared_ptr<NetTableTransaction>& merge_transaction,
    ChunkTransaction::Conflicts* conflicts) {
  CHECK_NOTNULL(merge_transaction.get());
  CHECK_NOTNULL(conflicts);
  conflicts->clear();
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    std::shared_ptr<ChunkTransaction> merge_chunk_transaction(
        new ChunkTransaction(merge_transaction->begin_time_,
                             chunk_transaction.first));
    ChunkTransaction::Conflicts sub_conflicts;
    chunk_transaction.second->merge(merge_chunk_transaction, &sub_conflicts);
    CHECK_EQ(chunk_transaction.second->numChangedItems(),
             merge_chunk_transaction->numChangedItems() + sub_conflicts.size());
    if (merge_chunk_transaction->numChangedItems() > 0u) {
      merge_transaction->chunk_transactions_.insert(
          std::make_pair(chunk_transaction.first, merge_chunk_transaction));
    }
    if (!sub_conflicts.empty()) {
      conflicts->splice(conflicts->end(), sub_conflicts);
    }
  }
}

size_t NetTableTransaction::numChangedItems() const {
  size_t result = 0;
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    result += chunk_transaction.second->numChangedItems();
  }
  return result;
}

ChunkTransaction* NetTableTransaction::transactionOf(Chunk* chunk) const {
  CHECK_NOTNULL(chunk);
  TransactionMap::iterator chunk_transaction = chunk_transactions_.find(chunk);
  if (chunk_transaction == chunk_transactions_.end()) {
    std::shared_ptr<ChunkTransaction> transaction(
        new ChunkTransaction(begin_time_, chunk));
    std::pair<TransactionMap::iterator, bool> inserted =
        chunk_transactions_.insert(std::make_pair(chunk, transaction));
    CHECK(inserted.second);
    chunk_transaction = inserted.first;
  }
  return chunk_transaction->second.get();
}

} /* namespace map_api */
