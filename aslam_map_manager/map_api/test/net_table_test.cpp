#include <string>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "map-api/ipc.h"
#include "map-api/net-table-transaction.h"
#include "map-api/transaction.h"

#include "net_table_test_fixture.cpp"

namespace map_api {

/**
 * Observation: A does all commits before B does all commits. This makes
 * sense because A's operations per transaction are less complex, thus
 * faster executed, and A gets to lock first.
 */
TEST_P(NetTableTest, NetTableTransactions) {
  if (!GetParam()) {
    return;
  }
  enum Processes {ROOT, A, B};
  enum Barriers {INIT, SYNC, DIE};
  int kCycles = 10;
  Id ab_chunk_id, b_chunk_id, ab_id, b_id;
  Chunk* ab_chunk, *b_chunk;
  if (getSubprocessId() == ROOT) {
    ab_chunk = table_->newChunk();
    b_chunk = table_->newChunk();
    ab_id = insert(0, ab_chunk);
    b_id = insert(0, b_chunk);
    launchSubprocess(A);
    launchSubprocess(B);

    IPC::barrier(INIT, 2);
    IPC::push(ab_chunk->id());
    IPC::push(b_chunk->id());
    IPC::push(ab_id);
    IPC::push(b_id);
    ab_chunk->requestParticipation();
    b_chunk->requestParticipation();

    IPC::barrier(SYNC, 2);
    IPC::barrier(DIE, 2);
    NetTableTransaction reader(table_);
    std::shared_ptr<Revision> ab_item = reader.getById(ab_id);
    std::shared_ptr<Revision> b_item = reader.getById(b_id);
    EXPECT_TRUE(ab_item->verifyEqual(kFieldName, 2 * kCycles));
    EXPECT_TRUE(b_item->verifyEqual(kFieldName, kCycles));
    EXPECT_EQ(kCycles + 2, count());
  }
  if (getSubprocessId() == A) {
    IPC::barrier(INIT, 2);
    IPC::barrier(SYNC, 2);
    IPC::pop(&ab_chunk_id);
    IPC::pop(&b_chunk_id);
    IPC::pop(&ab_id);
    ab_chunk = table_->getChunk(ab_chunk_id);
    for (int i = 0; i < kCycles; ++i) {
      while (true) {
        NetTableTransaction attempt(table_);
        increment(ab_id, ab_chunk, &attempt);
        std::shared_ptr<Revision> to_insert = table_->getTemplate();
        to_insert->set(CRTable::kIdField, Id::generate());
        to_insert->set(kFieldName, 42);
        attempt.insert(ab_chunk, to_insert);
        if (attempt.commit()) {
          break;
        }
      }
    }
    IPC::barrier(DIE, 2);
  }
  if (getSubprocessId() == B) {
    IPC::barrier(INIT, 2);
    IPC::barrier(SYNC, 2);
    IPC::pop(&ab_chunk_id);
    IPC::pop(&b_chunk_id);
    IPC::pop(&ab_id);
    IPC::pop(&b_id);
    ab_chunk = table_->getChunk(ab_chunk_id);
    b_chunk = table_->getChunk(b_chunk_id);
    for (int i = 0; i < kCycles; ++i) {
      while (true) {
        NetTableTransaction attempt(table_);
        increment(ab_id, ab_chunk, &attempt);
        increment(b_id, b_chunk, &attempt);
        if (attempt.commit()) {
          break;
        }
      }
    }
    IPC::barrier(DIE, 2);
  }
  LOG(INFO) << PeerId::self() << " done";
}

TEST_P(NetTableTest, Transactions) {
  if (!GetParam()) {
    return;
  }
  enum Processes {ROOT, A, B};
  enum Barriers {INIT, SYNC, DIE};
  int kCycles = 10;
  const std::string kSecondTableName = "net_transaction_test_table";
  const std::string kSecondTableFieldName = "n";

  std::unique_ptr<TableDescriptor> descriptor(new TableDescriptor);
  descriptor->setName(kSecondTableName);
  descriptor->addField<int>(kSecondTableFieldName);
  NetTableManager::instance().addTable(
      CRTable::Type::CRU, &descriptor);
  NetTable* second_table =
      &NetTableManager::instance().getTable(kSecondTableName);
  ASSERT_TRUE(second_table);

  Id ab_chunk_id, b_chunk_id, ab_id, b_id;
  Chunk* ab_chunk, *b_chunk;

  if (getSubprocessId() == ROOT) {
    ab_chunk = table_->newChunk();
    b_chunk = second_table->newChunk();
    ab_id = insert(0, ab_chunk);
    b_id = Id::generate();
    std::shared_ptr<Revision> to_insert = second_table->getTemplate();
    to_insert->set(CRTable::kIdField, b_id);
    to_insert->set(kSecondTableFieldName, 0);
    Transaction initial_insert;
    initial_insert.insert(second_table, b_chunk, to_insert);
    ASSERT_TRUE(initial_insert.commit());

    launchSubprocess(A);
    launchSubprocess(B);

    IPC::barrier(INIT, 2);
    IPC::push(ab_chunk->id());
    IPC::push(b_chunk->id());
    IPC::push(ab_id);
    IPC::push(b_id);
    ab_chunk->requestParticipation();
    b_chunk->requestParticipation();

    IPC::barrier(SYNC, 2);
    IPC::barrier(DIE, 2);
    Transaction reader;
    EXPECT_TRUE(reader.getById(ab_id, table_, ab_chunk)
                    ->verifyEqual(kFieldName, 2 * kCycles));
    EXPECT_TRUE(reader.getById(b_id, second_table, b_chunk)
                    ->verifyEqual(kSecondTableFieldName, kCycles));
    EXPECT_EQ(kCycles + 1, count());
  }
  if (getSubprocessId() == A) {
    IPC::barrier(INIT, 2);
    IPC::barrier(SYNC, 2);
    IPC::pop(&ab_chunk_id);
    ab_chunk = table_->getChunk(ab_chunk_id);
    IPC::pop(&b_chunk_id);
    IPC::pop(&ab_id);
    for (int i = 0; i < kCycles; ++i) {
      while (true) {
        Transaction attempt;
        increment(table_, ab_id, ab_chunk, &attempt);
        std::shared_ptr<Revision> to_insert = table_->getTemplate();
        to_insert->set(CRTable::kIdField, Id::generate());
        to_insert->set(kFieldName, 42);
        attempt.insert(table_, ab_chunk, to_insert);
        if (attempt.commit()) {
          break;
        }
      }
    }
    IPC::barrier(DIE, 2);
  }
  if (getSubprocessId() == B) {
    IPC::barrier(INIT, 2);
    IPC::barrier(SYNC, 2);
    IPC::pop(&ab_chunk_id);
    IPC::pop(&b_chunk_id);
    IPC::pop(&ab_id);
    IPC::pop(&b_id);
    ab_chunk = table_->getChunk(ab_chunk_id);
    b_chunk = second_table->getChunk(b_chunk_id);
    for (int i = 0; i < kCycles; ++i) {
      while (true) {
        Transaction attempt;
        increment(table_, ab_id, ab_chunk, &attempt);
        CRTable::RevisionMap chunk_dump =
            attempt.dumpChunk(second_table, b_chunk);
        CRTable::RevisionMap::iterator found = chunk_dump.find(b_id);
        std::shared_ptr<Revision> to_update = found->second;
        int transient_value;
        to_update->get(kSecondTableFieldName, &transient_value);
        ++transient_value;
        to_update->set(kSecondTableFieldName, transient_value);
        attempt.update(second_table, to_update);
        if (attempt.commit()) {
          break;
        }
      }
    }
    IPC::barrier(DIE, 2);
  }
}

TEST_P(NetTableTest, CommitTime) {
  if (!GetParam()) {
    return;
  }
  Chunk* chunk = table_->newChunk();
  Transaction transaction;
  // TODO(tcies) factor insertion into a NetTableTest function
  std::shared_ptr<Revision> to_insert_1 = table_->getTemplate();
  to_insert_1->set(CRTable::kIdField, Id::generate());
  to_insert_1->set(kFieldName, 42);
  std::shared_ptr<Revision> to_insert_2 = table_->getTemplate();
  to_insert_2->set(CRTable::kIdField, Id::generate());
  to_insert_2->set(kFieldName, 21);
  transaction.insert(table_, chunk, to_insert_1);
  transaction.insert(table_, chunk, to_insert_2);
  ASSERT_TRUE(transaction.commit());
  CRTable::RevisionMap retrieved;
  chunk->dumpItems(LogicalTime::sample(), &retrieved);
  ASSERT_EQ(2, retrieved.size());
  CRTable::RevisionMap::iterator it = retrieved.begin();
  LogicalTime time_1, time_2;
  it->second->get(CRTable::kInsertTimeField, &time_1);
  ++it;
  it->second->get(CRTable::kInsertTimeField, &time_2);
  EXPECT_EQ(time_1, time_2);
  // TODO(tcies) also test update times, and times accross multiple chunks
}

TEST_P(NetTableTest, ChunkLookup) {
  if (GetParam()) {
    return;  // independent of updateability
  }
  enum Processes {MASTER, SLAVE};
  enum Barriers {INIT, CHUNK_CREATED, DIE};
  Chunk* chunk;
  CRTable::RevisionMap results;
  if (getSubprocessId() == MASTER) {
    launchSubprocess(SLAVE);
    IPC::barrier(INIT, 1);
    IPC::barrier(CHUNK_CREATED, 1);
    table_->dumpActiveChunksAtCurrentTime(&results);
    EXPECT_EQ(0, results.size());
    Id chunk_id;
    IPC::pop(&chunk_id);
    chunk = table_->getChunk(chunk_id);
    EXPECT_TRUE(chunk);
    table_->dumpActiveChunksAtCurrentTime(&results);
    EXPECT_EQ(1, results.size());
  }
  if (getSubprocessId() == SLAVE) {
    IPC::barrier(INIT, 1);
    chunk = table_->newChunk();
    EXPECT_TRUE(chunk);
    insert(0, chunk);
    IPC::push(chunk->id().hexString());
    IPC::barrier(CHUNK_CREATED, 1);
  }
  IPC::barrier(DIE, 1);
}

}  // namespace map_api

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
