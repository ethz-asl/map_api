#include "map-api/transaction.h"

#include <timing/timer.h>

#include "map-api/cache-base.h"
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

void Transaction::remove(NetTable* table, std::shared_ptr<Revision> revision) {
  transactionOf(CHECK_NOTNULL(table))->remove(revision);
}

// Deadlocks are prevented by imposing a global ordering on
// net_table_transactions_, and have the locks acquired in that order
// (resource hierarchy solution)
bool Transaction::commit() {
  for (const CacheMap::value_type& cache_pair : attached_caches_) {
    cache_pair.second->prepareForCommit();
  }
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
  commit_time_ = LogicalTime::sample();
  for (const TransactionPair& net_table_transaction : net_table_transactions_) {
    net_table_transaction.second->checkedCommit(commit_time_);
    net_table_transaction.second->unlock();
  }
  return true;
}

void Transaction::merge(const std::shared_ptr<Transaction>& merge_transaction,
                        ConflictMap* conflicts) {
  CHECK(merge_transaction.get() != nullptr) << "Merge requires an initiated "
                                               "transaction";
  CHECK_NOTNULL(conflicts);
  conflicts->clear();
  for (const TransactionPair& net_table_transaction : net_table_transactions_) {
    std::shared_ptr<NetTableTransaction> merge_net_table_transaction(
        new NetTableTransaction(merge_transaction->begin_time_,
                                net_table_transaction.first));
    ChunkTransaction::Conflicts sub_conflicts;
    net_table_transaction.second->merge(merge_net_table_transaction,
                                        &sub_conflicts);
    CHECK_EQ(
        net_table_transaction.second->numChangedItems(),
        merge_net_table_transaction->numChangedItems() + sub_conflicts.size());
    if (merge_net_table_transaction->numChangedItems() > 0u) {
      merge_transaction->net_table_transactions_.insert(std::make_pair(
          net_table_transaction.first, merge_net_table_transaction));
    }
    if (!sub_conflicts.empty()) {
      std::pair<ConflictMap::iterator, bool> insert_result =
          conflicts->insert(std::make_pair(net_table_transaction.first,
                                           ChunkTransaction::Conflicts()));
      CHECK(insert_result.second);
      insert_result.first->second.swap(sub_conflicts);
    }
  }
}

size_t Transaction::numChangedItems() const {
  size_t count = 0u;
  for (const TransactionPair& net_table_transaction : net_table_transactions_) {
    count += net_table_transaction.second->numChangedItems();
  }
  return count;
}

void Transaction::attachCache(NetTable* table, CacheBase* cache) {
  CHECK_NOTNULL(table);
  CHECK_NOTNULL(cache);
  ensureAccessIsCache(table);
  attached_caches_.emplace(table, cache);
}

void Transaction::enableDirectAccessForCache() {
  CHECK(cache_access_override_.insert(std::this_thread::get_id()).second);
}

void Transaction::disableDirectAccessForCache() {
  CHECK_EQ(1u, cache_access_override_.erase(std::this_thread::get_id()));
}

NetTableTransaction* Transaction::transactionOf(NetTable* table) const {
  CHECK_NOTNULL(table);
  ensureAccessIsDirect(table);
  TransactionMap::const_iterator net_table_transaction =
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

void Transaction::ensureAccessIsCache(NetTable* table) const {
  TableAccessModeMap::iterator found = access_mode_.find(table);
  if (found == access_mode_.end()) {
    access_mode_[table] = TableAccessMode::kCache;
  } else {
    CHECK(found->second == TableAccessMode::kCache)
        << "Access mode for table " << table->name() << " is already direct, "
                                                        "may not attach cache.";
  }
}

void Transaction::ensureAccessIsDirect(NetTable* table) const {
  TableAccessModeMap::iterator found = access_mode_.find(table);
  if (found == access_mode_.end()) {
    access_mode_[table] = TableAccessMode::kDirect;
  } else {
    if (found->second != TableAccessMode::kDirect) {
      CHECK(cache_access_override_.find(std::this_thread::get_id()) !=
            cache_access_override_.end())
          << "Access mode for table " << table->name()
          << " is already by cache, may not access directly.";
    }
  }
}

} /* namespace map_api */
