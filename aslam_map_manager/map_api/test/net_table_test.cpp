#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "map-api/ipc.h"
#include "map-api/net-table-transaction.h"
#include "map-api/transaction.h"

#include "net_table_test_fixture.cpp"

using namespace map_api;

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
  if (getSubprocessId() == ROOT) {
    Chunk* ab_chunk = table_->newChunk(), *b_chunk = table_->newChunk();
    Id ab_id = insert(0, ab_chunk), b_id = insert(0, b_chunk);
    launchSubprocess(A);
    launchSubprocess(B);

    IPC::barrier(INIT, 2);
    IPC::push(ab_chunk->id().hexString());
    IPC::push(ab_id.hexString());
    IPC::push(b_id.hexString());
    ab_chunk->requestParticipation();
    b_chunk->requestParticipation();

    IPC::barrier(SYNC, 2);
    IPC::barrier(DIE, 2);
    EXPECT_TRUE(table_->getById(ab_id, LogicalTime::sample())->
                verify(kFieldName, 2 * kCycles));
    EXPECT_TRUE(table_->getById(b_id, LogicalTime::sample())->
                verify(kFieldName, kCycles));
    EXPECT_EQ(kCycles + 2, count());
  }
  if (getSubprocessId() == A) {
    IPC::barrier(INIT, 2);
    IPC::barrier(SYNC, 2);
    Id chunk_id = popId(), ab_id = popId();
    for (int i = 0; i < kCycles; ++i) {
      while (true) {
        NetTableTransaction attempt(table_);
        increment(ab_id, &attempt);
        std::shared_ptr<Revision> to_insert = table_->getTemplate();
        to_insert->set(CRTable::kIdField, Id::generate());
        to_insert->set(kFieldName, 42);
        attempt.insert(table_->getChunk(chunk_id), to_insert);
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
    popId();
    Id ab_id = popId(), b_id = popId();
    for (int i = 0; i < kCycles; ++i) {
      while (true) {
        NetTableTransaction attempt(table_);
        increment(ab_id, &attempt);
        increment(b_id, &attempt);
        if (attempt.commit()) {
          break;
        }
      }
    }
    IPC::barrier(DIE, 2);
  }
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
  MapApiCore::instance().tableManager().addTable(true, &descriptor);
  NetTable* second_table =
      &MapApiCore::instance().tableManager().getTable(kSecondTableName);
  ASSERT_TRUE(second_table);

  if (getSubprocessId() == ROOT) {
    Chunk* ab_chunk = table_->newChunk(), *b_chunk = second_table->newChunk();
    Id ab_id = insert(0, ab_chunk), b_id;
    b_id = Id::generate();
    std::shared_ptr<Revision> to_insert = second_table->getTemplate();
    to_insert->set(CRTable::kIdField, b_id);
    to_insert->set(kSecondTableFieldName, 0);
    EXPECT_TRUE(second_table->insert(b_chunk, to_insert.get()));

    launchSubprocess(A);
    launchSubprocess(B);

    IPC::barrier(INIT, 2);
    IPC::push(ab_chunk->id().hexString());
    IPC::push(ab_id.hexString());
    IPC::push(b_id.hexString());
    ab_chunk->requestParticipation();
    b_chunk->requestParticipation();

    IPC::barrier(SYNC, 2);
    IPC::barrier(DIE, 2);
    EXPECT_TRUE(table_->getById(ab_id, LogicalTime::sample())->
                verify(kFieldName, 2 * kCycles));
    EXPECT_TRUE(second_table->getById(b_id, LogicalTime::sample())->
                verify(kSecondTableFieldName, kCycles));
    EXPECT_EQ(kCycles + 1, count());
  }
  if (getSubprocessId() == A) {
    IPC::barrier(INIT, 2);
    IPC::barrier(SYNC, 2);
    Id chunk_id = popId(), ab_id = popId();
    for (int i = 0; i < kCycles; ++i) {
      while (true) {
        Transaction attempt;
        increment(table_, ab_id, &attempt);
        std::shared_ptr<Revision> to_insert = table_->getTemplate();
        to_insert->set(CRTable::kIdField, Id::generate());
        to_insert->set(kFieldName, 42);
        attempt.insert(table_, table_->getChunk(chunk_id), to_insert);
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
    popId();
    Id ab_id = popId(), b_id = popId();
    for (int i = 0; i < kCycles; ++i) {
      while (true) {
        Transaction attempt;
        increment(table_, ab_id, &attempt);
        std::shared_ptr<Revision> to_update =
            attempt.getById(second_table, b_id);
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

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
