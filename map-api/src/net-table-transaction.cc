#include "map-api/net-table-transaction.h"

#include <statistics/statistics.h>

#include "map-api/conflicts.h"
#include "map-api/raft-chunk.h"

#include "./raft.pb.h"

DEFINE_bool(map_api_dump_available_chunk_contents, false,
            "Will print all available ids if enabled.");

DEFINE_bool(map_api_blame_updates, false,
            "Print update counts per chunk per table.");

namespace map_api {

NetTableTransaction::NetTableTransaction(const LogicalTime& begin_time,
                                         NetTable* table,
                                         const Workspace& workspace)
    : begin_time_(begin_time),
      table_(table),
      workspace_(Workspace::TableInterface(workspace, table)) {
  CHECK_NOTNULL(table);
  CHECK(begin_time < LogicalTime::sample());

  refreshIdToChunkIdMap();
}

void NetTableTransaction::dumpChunk(const ChunkBase* chunk,
                                    ConstRevisionMap* result) {
  CHECK_NOTNULL(chunk);
  if (workspace_.contains(chunk->id())) {
    transactionOf(chunk)->dumpChunk(result);
  } else {
    result->clear();
  }
}

void NetTableTransaction::dumpActiveChunks(ConstRevisionMap* result) {
  CHECK_NOTNULL(result);
  workspace_.forEachChunk([&, this](const ChunkBase& chunk) {
    ConstRevisionMap chunk_revisions;
    dumpChunk(&chunk, &chunk_revisions);
    result->insert(chunk_revisions.begin(), chunk_revisions.end());
  });
}

void NetTableTransaction::insert(ChunkBase* chunk,
                                 std::shared_ptr<Revision> revision) {
  CHECK_NOTNULL(chunk);
  transactionOf(chunk)->insert(revision);
  CHECK(item_id_to_chunk_id_map_.emplace(revision->getId<common::Id>(),
                                         chunk->id()).second);
}

void NetTableTransaction::update(std::shared_ptr<Revision> revision) {
  common::Id id = revision->getId<common::Id>();
  CHECK(id.isValid());
  ChunkBase* chunk = chunkOf(id);
  CHECK_NOTNULL(chunk);
  if (revision->getChunkId().isValid()) {
    CHECK_EQ(chunk->id(), revision->getChunkId());
  }
  transactionOf(chunk)->update(revision);
}

void NetTableTransaction::remove(std::shared_ptr<Revision> revision) {
  ChunkBase* chunk = chunkOf(revision->getId<common::Id>());
  CHECK_NOTNULL(chunk);
  CHECK_EQ(chunk->id(), revision->getChunkId());
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

bool NetTableTransaction::checkedCommit(const LogicalTime& time) {
  if (FLAGS_map_api_blame_updates) {
    std::cout << "Updates in table " << table_->name() << ":" << std::endl;
  }
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    if (!chunk_transaction.second->checkedCommit(time)) {
      return false;
    }
  }
  return true;
}

void NetTableTransaction::prepareMultiChunkTransactionInfo(
    proto::MultiChunkTransactionInfo* info) {
  CHECK(FLAGS_use_raft);
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    proto::ChunkRequestMetadata chunk_metadata;
    chunk_metadata.set_table(table_->name());
    chunk_transaction.second->chunk_->id().serialize(
        chunk_metadata.mutable_chunk_id());
    info->add_chunk_list()->CopyFrom(chunk_metadata);

    // TODO(aqurai): To be removed. (Issue #2466)
    const PeerId& leader = CHECK_NOTNULL(
        dynamic_cast<RaftChunk*>(chunk_transaction.first))  // NOLINT
                               ->raft_node_.getLeader();
    CHECK(leader.isValid());
    info->add_leader_id(leader.ipPort());
  }
}

bool NetTableTransaction::sendMultiChunkTransactionInfo(
    const proto::MultiChunkTransactionInfo& info) {
  CHECK(FLAGS_use_raft);
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    if (!chunk_transaction.second->sendMultiChunkTransactionInfo(info)) {
      return false;
    }
  }
  return true;
}

// Deadlocks in lock() are prevented by imposing a global ordering on chunks,
// and have the locks acquired in that order (resource hierarchy solution)
void NetTableTransaction::lock() {
  size_t i = 0u;
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    chunk_transaction.first->writeLock();
    chunk_transaction.second->locked_by_transaction_ = true;
    ++i;
  }
  statistics::StatsCollector stat("map_api::NetTableTransaction::lock - " +
                                  table_->name());
  stat.AddSample(i);
}

void NetTableTransaction::unlock() {
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    if (chunk_transaction.second->locked_by_transaction_) {
      chunk_transaction.first->unlock();
      chunk_transaction.second->locked_by_transaction_ = false;
    }
  }
}

void NetTableTransaction::unlock(bool is_success) {
  CHECK(FLAGS_use_raft);
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    if (chunk_transaction.second->locked_by_transaction_) {
      // TODO(aqurai): Add a function to ChunkBase and avoid dynamic_cast?
      CHECK_NOTNULL(
          dynamic_cast<RaftChunk*>(chunk_transaction.first))  // NOLINT
          ->unlock(is_success);
      chunk_transaction.second->locked_by_transaction_ = false;
    }
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
    Conflicts* conflicts) {
  CHECK_NOTNULL(merge_transaction.get());
  CHECK_NOTNULL(conflicts);
  conflicts->clear();
  for (const TransactionPair& chunk_transaction : chunk_transactions_) {
    std::shared_ptr<ChunkTransaction> merge_chunk_transaction(
        new ChunkTransaction(merge_transaction->begin_time_,
                             chunk_transaction.first, table_));
    Conflicts sub_conflicts;
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

ChunkTransaction* NetTableTransaction::transactionOf(const ChunkBase* chunk)
    const {
  CHECK_NOTNULL(chunk);
  // Const cast needed, as transactions map has non-const key.
  ChunkBase* mutable_chunk = const_cast<ChunkBase*>(chunk);
  TransactionMap::iterator chunk_transaction =
      chunk_transactions_.find(mutable_chunk);
  if (chunk_transaction == chunk_transactions_.end()) {
    std::shared_ptr<ChunkTransaction> transaction(
        new ChunkTransaction(begin_time_, mutable_chunk, table_));
    std::pair<TransactionMap::iterator, bool> inserted =
        chunk_transactions_.insert(std::make_pair(mutable_chunk, transaction));
    CHECK(inserted.second);
    chunk_transaction = inserted.first;
  }
  return chunk_transaction->second.get();
}

void NetTableTransaction::refreshIdToChunkIdMap() {
  item_id_to_chunk_id_map_.clear();
  if (FLAGS_map_api_dump_available_chunk_contents) {
    std::cout << table_->name() << " chunk contents:" << std::endl;
  }
  workspace_.forEachChunk([&, this](const ChunkBase& chunk) {
    std::vector<common::Id> chunk_result;
    chunk.constData()->getAvailableIds(begin_time_, &chunk_result);
    if (FLAGS_map_api_dump_available_chunk_contents) {
      std::cout << "\tChunk " << chunk.id().hexString() << ":" << std::endl;
    }
    for (const common::Id& item_id : chunk_result) {
      CHECK(item_id_to_chunk_id_map_.emplace(item_id, chunk.id()).second);
      if (FLAGS_map_api_dump_available_chunk_contents) {
        std::cout << "\t\tItem " << item_id.hexString() << std::endl;
      }
    }
  });
}

void NetTableTransaction::getChunkTrackers(
    TrackedChunkToTrackersMap* chunk_trackers) const {
  CHECK_NOTNULL(chunk_trackers);
  for (const TransactionMap::value_type& chunk_transaction :
       chunk_transactions_) {
    chunk_transaction.second->getTrackers(
        push_new_chunk_ids_to_tracker_overrides_,
        &(*chunk_trackers)[chunk_transaction.first->id()]);
  }
}

} /* namespace map_api */
