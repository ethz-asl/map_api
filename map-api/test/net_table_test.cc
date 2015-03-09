#include <string>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "map-api/ipc.h"
#include "map-api/net-table-manager.h"
#include "map-api/net-table-transaction.h"
#include "map-api/test/testing-entrypoint.h"
#include "map-api/transaction.h"
#include "./net_table_fixture.h"

namespace map_api {

/**
 * Observation: A does all commits before B does all commits. This makes
 * sense because A's operations per transaction are less complex, thus
 * faster executed, and A gets to lock first.
 */
TEST_P(NetTableFixture, NetTableTransactions) {
  if (!GetParam()) {
    return;
  }
  enum Processes {
    ROOT,
    A,
    B
  };
  enum Barriers {
    INIT,
    SYNC,
    DIE
  };
  int kCycles = 10;
  common::Id ab_chunk_id, b_chunk_id, ab_id, b_id;
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
    std::shared_ptr<const Revision> ab_item = reader.getById(ab_id);
    std::shared_ptr<const Revision> b_item = reader.getById(b_id);
    EXPECT_TRUE(ab_item->verifyEqual(kFieldName, 2 * kCycles));
    EXPECT_TRUE(b_item->verifyEqual(kFieldName, kCycles));
    EXPECT_EQ(kCycles + 2, static_cast<int>(count()));
  }
  if (getSubprocessId() == A) {
    IPC::barrier(INIT, 2);
    IPC::barrier(SYNC, 2);
    ab_chunk_id = IPC::pop<common::Id>();
    b_chunk_id = IPC::pop<common::Id>();
    ab_id = IPC::pop<common::Id>();
    ab_chunk = table_->getChunk(ab_chunk_id);
    for (int i = 0; i < kCycles; ++i) {
      while (true) {
        NetTableTransaction attempt(table_);
        increment(ab_id, ab_chunk, &attempt);
        std::shared_ptr<Revision> to_insert = table_->getTemplate();
        common::Id insert_id;
        generateId(&insert_id);
        to_insert->setId(insert_id);
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
    ab_chunk_id = IPC::pop<common::Id>();
    b_chunk_id = IPC::pop<common::Id>();
    ab_id = IPC::pop<common::Id>();
    b_id = IPC::pop<common::Id>();
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

TEST_P(NetTableFixture, Transactions) {
  if (!GetParam()) {
    return;
  }
  enum Processes {
    ROOT,
    A,
    B
  };
  enum Barriers {
    INIT,
    SYNC,
    DIE
  };
  int kCycles = 10;
  const std::string kSecondTableName = "net_transaction_test_table";
  enum Fields {
    kSecondTableFieldName
  };

  std::unique_ptr<TableDescriptor> descriptor(new TableDescriptor);
  descriptor->setName(kSecondTableName);
  descriptor->addField<int>(kSecondTableFieldName);
  NetTable* second_table =
      NetTableManager::instance().addTable(CRTable::Type::CRU, &descriptor);
  ASSERT_TRUE(second_table);

  common::Id ab_chunk_id, b_chunk_id, ab_id, b_id;
  Chunk* ab_chunk, *b_chunk;

  if (getSubprocessId() == ROOT) {
    ab_chunk = table_->newChunk();
    b_chunk = second_table->newChunk();
    ab_id = insert(0, ab_chunk);
    generateId(&b_id);
    std::shared_ptr<Revision> to_insert = second_table->getTemplate();
    to_insert->setId(b_id);
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
    EXPECT_EQ(kCycles + 1, static_cast<int>(count()));
  }
  if (getSubprocessId() == A) {
    IPC::barrier(INIT, 2);
    IPC::barrier(SYNC, 2);
    ab_chunk_id = IPC::pop<common::Id>();
    ab_chunk = table_->getChunk(ab_chunk_id);
    b_chunk_id = IPC::pop<common::Id>();
    ab_id = IPC::pop<common::Id>();
    for (int i = 0; i < kCycles; ++i) {
      while (true) {
        Transaction attempt;
        increment(table_, ab_id, ab_chunk, &attempt);
        std::shared_ptr<Revision> to_insert = table_->getTemplate();
        common::Id insert_id;
        generateId(&insert_id);
        to_insert->setId(insert_id);
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
    ab_chunk_id = IPC::pop<common::Id>();
    b_chunk_id = IPC::pop<common::Id>();
    ab_id = IPC::pop<common::Id>();
    b_id = IPC::pop<common::Id>();
    ab_chunk = table_->getChunk(ab_chunk_id);
    b_chunk = second_table->getChunk(b_chunk_id);
    for (int i = 0; i < kCycles; ++i) {
      while (true) {
        Transaction attempt;
        increment(table_, ab_id, ab_chunk, &attempt);
        CRTable::RevisionMap chunk_dump =
            attempt.dumpChunk(second_table, b_chunk);
        CRTable::RevisionMap::iterator found = chunk_dump.find(b_id);
        std::shared_ptr<Revision> to_update = found->second->copyForWrite();
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

TEST_P(NetTableFixture, CommitTime) {
  if (!GetParam()) {
    return;
  }
  Chunk* chunk = table_->newChunk();
  Transaction transaction;
  // TODO(tcies) factor insertion into a NetTableTest function
  std::shared_ptr<Revision> to_insert_1 = table_->getTemplate();
  common::Id insert_id;
  generateId(&insert_id);
  to_insert_1->setId(insert_id);
  to_insert_1->set(kFieldName, 42);
  std::shared_ptr<Revision> to_insert_2 = table_->getTemplate();
  generateId(&insert_id);
  to_insert_2->setId(insert_id);
  to_insert_2->set(kFieldName, 21);
  transaction.insert(table_, chunk, to_insert_1);
  transaction.insert(table_, chunk, to_insert_2);
  ASSERT_TRUE(transaction.commit());
  CRTable::RevisionMap retrieved;
  chunk->dumpItems(LogicalTime::sample(), &retrieved);
  ASSERT_EQ(2u, retrieved.size());
  CRTable::RevisionMap::iterator it = retrieved.begin();
  LogicalTime time_1 = it->second->getInsertTime();
  ++it;
  LogicalTime time_2 = it->second->getInsertTime();
  EXPECT_EQ(time_1, time_2);
  // TODO(tcies) also test update times, and times accross multiple chunks
}

TEST_P(NetTableFixture, ChunkLookup) {
  if (GetParam()) {
    return;  // independent of updateability
  }
  enum Processes {
    MASTER,
    SLAVE
  };
  enum Barriers {
    INIT,
    CHUNK_CREATED,
    DIE
  };
  Chunk* chunk;
  CRTable::RevisionMap results;
  if (getSubprocessId() == MASTER) {
    launchSubprocess(SLAVE);
    IPC::barrier(INIT, 1);
    IPC::barrier(CHUNK_CREATED, 1);
    table_->dumpActiveChunksAtCurrentTime(&results);
    EXPECT_EQ(0u, results.size());
    common::Id chunk_id;
    chunk_id = IPC::pop<common::Id>();
    chunk = table_->getChunk(chunk_id);
    EXPECT_TRUE(chunk);
    table_->dumpActiveChunksAtCurrentTime(&results);
    EXPECT_EQ(1u, results.size());
  }
  if (getSubprocessId() == SLAVE) {
    IPC::barrier(INIT, 1);
    chunk = table_->newChunk();
    EXPECT_TRUE(chunk);
    insert(0, chunk);
    IPC::push(chunk->id());
    IPC::barrier(CHUNK_CREATED, 1);
  }
  IPC::barrier(DIE, 1);
}

TEST_P(NetTableFixture, ListenToChunksFromPeer) {
  if (GetParam()) {
    return;  // Independent of whether CR or CRUD.
  }
  enum Processes {
    MASTER,
    SLAVE
  };
  enum Barriers {
    ADDRESS_SHARED,
    LISTENING,
    CHUNKS_CREATED,
    DIE
  };
  if (getSubprocessId() == MASTER) {
    launchSubprocess(SLAVE);
    IPC::barrier(ADDRESS_SHARED, 1);
    PeerId peer = IPC::pop<PeerId>();
    table_->listenToChunksFromPeer(peer);
    IPC::barrier(LISTENING, 1);
    IPC::barrier(CHUNKS_CREATED, 1);
    usleep(50000);  // Should suffice for auto-fetching.
    IPC::barrier(DIE, 1);
    EXPECT_EQ(2u, table_->numActiveChunks());
  }
  if (getSubprocessId() == SLAVE) {
    IPC::push(PeerId::self());
    IPC::barrier(ADDRESS_SHARED, 1);
    table_->newChunk();
    IPC::barrier(LISTENING, 1);
    table_->newChunk();
    IPC::barrier(CHUNKS_CREATED, 1);
    IPC::barrier(DIE, 1);
  }
}

TEST_P(NetTableFixture, ListenToNewPeersOfTable) {
  if (GetParam()) {
    return;  // Independent of whether CR or CRUD.
  }
  enum Processes {
    MASTER,
    SLAVE
  };
  enum Barriers {
    CHUNK_CREATED,
    DIE
  };
  if (getSubprocessId() == MASTER) {
    // Currently, it is only possible to listen to peers joining the table
    // in the future.
    NetTableManager::instance().listenToPeersJoiningTable(table_->name());
    launchSubprocess(SLAVE);
    IPC::barrier(CHUNK_CREATED, 1);
    usleep(50000);  // Should suffice for auto-fetching.
    IPC::barrier(DIE, 1);
    EXPECT_EQ(1u, table_->numActiveChunks());
  }
  if (getSubprocessId() == SLAVE) {
    table_->newChunk();
    IPC::barrier(CHUNK_CREATED, 1);
    IPC::barrier(DIE, 1);
  }
}

class NetTableChunkTrackingTest : public NetTableFixture {
 protected:
  enum Processes {
    MASTER,
    SLAVE
  };
  static const std::string kTrackeeTableName;
  enum TrackeeTableFields {
    kParent
  };
  // Static const member because GTEST methods can't handle constexpr.
  static const size_t kNumTrackeeChunks;

  static common::Id get_tracker(const Revision& item) {
    common::Id result;
    item.get(kParent, &result);
    return result;
  }

  void SetUp() {
    NetTableFixture::SetUp();
    std::unique_ptr<TableDescriptor> descriptor(new TableDescriptor);
    descriptor->setName(kTrackeeTableName);
    descriptor->addField<common::Id>(kParent);
    trackee_table_ =
        NetTableManager::instance().addTable(CRTable::Type::CR, &descriptor);
    trackee_table_->pushNewChunkIdsToTracker(table_, get_tracker);
    generateIdFromInt(1, &master_chunk_id_);
    generateIdFromInt(1, &master_item_id_);
  }

  void insert_master_item(Transaction* transaction) {
    chunk_ = table_->newChunk(master_chunk_id_);
    std::shared_ptr<Revision> to_insert = table_->getTemplate();
    to_insert->setId(master_item_id_);
    to_insert->set(kFieldName, 0);
    transaction->insert(table_, chunk_, to_insert);
  }

  void insert_trackees(Transaction* transaction) {
    for (size_t i = 0; i < kNumTrackeeChunks; ++i) {
      Chunk* trackee_chunk = trackee_table_->newChunk();
      std::shared_ptr<Revision> to_insert = trackee_table_->getTemplate();
      common::Id id;
      generateId(&id);
      to_insert->setId(id);
      to_insert->set(kParent, master_item_id_);
      transaction->insert(trackee_table_, trackee_chunk, to_insert);
    }
  }

  void fetch_trackees() {
    Transaction transaction;
    chunk_ = table_->getChunk(master_chunk_id_);
    EXPECT_NE(chunk_, nullptr);
    std::shared_ptr<const Revision> master_item =
        transaction.getById(master_item_id_, table_, chunk_);
    EXPECT_NE(master_item.get(), nullptr);
    master_item->fetchTrackedChunks();
  }

  void follow_trackees() {
    chunk_ = table_->getChunk(master_chunk_id_);
    ASSERT_NE(chunk_, nullptr);
    table_->followTrackedChunksOfItem(master_item_id_, chunk_);
  }

  NetTable* trackee_table_;
  common::Id master_chunk_id_, master_item_id_;
};

const std::string NetTableChunkTrackingTest::kTrackeeTableName =
    "trackee_table";
const size_t NetTableChunkTrackingTest::kNumTrackeeChunks = 10;

INSTANTIATE_TEST_CASE_P(Default, NetTableChunkTrackingTest,
                        ::testing::Values(true));

TEST_P(NetTableChunkTrackingTest, ChunkTrackingSameTransaction) {
  enum Barriers {
    INIT,
    SLAVE_DONE,
    DIE
  };
  if (getSubprocessId() == MASTER) {
    launchSubprocess(SLAVE);
    IPC::barrier(INIT, 1);
    IPC::barrier(SLAVE_DONE, 1);
    EXPECT_EQ(0u, trackee_table_->numActiveChunks());
    fetch_trackees();
    EXPECT_EQ(kNumTrackeeChunks, trackee_table_->numActiveChunks());
    EXPECT_EQ(kNumTrackeeChunks, trackee_table_->numItems());
  }
  if (getSubprocessId() == SLAVE) {
    IPC::barrier(INIT, 1);
    Transaction slave_transaction;
    insert_master_item(&slave_transaction);
    insert_trackees(&slave_transaction);
    EXPECT_TRUE(slave_transaction.commit());
    IPC::barrier(SLAVE_DONE, 1);
  }
  IPC::barrier(DIE, 1);
}

TEST_P(NetTableChunkTrackingTest, ChunkTrackingDifferentTransaction) {
  enum Barriers {
    INIT,
    TRACKER_DONE,
    TRACKEES_DONE,
    DIE
  };
  if (getSubprocessId() == MASTER) {
    launchSubprocess(SLAVE);
    IPC::barrier(INIT, 1);
    IPC::barrier(TRACKER_DONE, 1);
    fetch_trackees();
    EXPECT_EQ(0u, trackee_table_->numActiveChunks());
    IPC::barrier(TRACKEES_DONE, 1);
    fetch_trackees();
    EXPECT_EQ(kNumTrackeeChunks, trackee_table_->numActiveChunks());
    EXPECT_EQ(kNumTrackeeChunks, trackee_table_->numItems());
  }
  if (getSubprocessId() == SLAVE) {
    IPC::barrier(INIT, 1);
    Transaction slave_transaction_1;
    insert_master_item(&slave_transaction_1);
    EXPECT_TRUE(slave_transaction_1.commit());
    IPC::barrier(TRACKER_DONE, 1);
    Transaction slave_transaction_2;
    insert_trackees(&slave_transaction_2);
    EXPECT_TRUE(slave_transaction_2.commit());
    IPC::barrier(TRACKEES_DONE, 1);
  }
  IPC::barrier(DIE, 1);
}

TEST_P(NetTableChunkTrackingTest, FollowTrackedChunks) {
  enum Barriers {
    INIT,
    TRACKER_DONE,
    TRACKER_READ,
    TRACKEES_DONE,
    DIE
  };
  if (getSubprocessId() == MASTER) {
    launchSubprocess(SLAVE);
    IPC::barrier(INIT, 1);
    IPC::barrier(TRACKER_DONE, 1);
    follow_trackees();
    EXPECT_EQ(0u, trackee_table_->numActiveChunks());
    IPC::barrier(TRACKER_READ, 1);
    IPC::barrier(TRACKEES_DONE, 1);
    chunk_->waitForTriggerCompletion();
    EXPECT_EQ(kNumTrackeeChunks, trackee_table_->numActiveChunks());
    EXPECT_EQ(kNumTrackeeChunks, trackee_table_->numItems());
  }
  if (getSubprocessId() == SLAVE) {
    IPC::barrier(INIT, 1);
    Transaction slave_transaction_1;
    insert_master_item(&slave_transaction_1);
    EXPECT_TRUE(slave_transaction_1.commit());
    IPC::barrier(TRACKER_DONE, 1);
    IPC::barrier(TRACKER_READ, 1);
    Transaction slave_transaction_2;
    insert_trackees(&slave_transaction_2);
    EXPECT_TRUE(slave_transaction_2.commit());
    IPC::barrier(TRACKEES_DONE, 1);
  }
  IPC::barrier(DIE, 1);
}

TEST_P(NetTableChunkTrackingTest, AutoFollowTrackedChunks) {
  enum Barriers {
    INIT,
    TRACKER_DONE,
    TRACKER_READ,
    TRACKEES_DONE,
    DIE
  };
  if (getSubprocessId() == MASTER) {
    table_->autoFollowTrackedChunks();
    launchSubprocess(SLAVE);
    IPC::barrier(INIT, 1);
    IPC::barrier(TRACKER_DONE, 1);
    chunk_ = table_->getChunk(master_chunk_id_);
    chunk_->waitForTriggerCompletion();
    EXPECT_EQ(0u, trackee_table_->numActiveChunks());
    IPC::barrier(TRACKER_READ, 1);
    IPC::barrier(TRACKEES_DONE, 1);
    chunk_->waitForTriggerCompletion();
    EXPECT_EQ(kNumTrackeeChunks, trackee_table_->numActiveChunks());
    EXPECT_EQ(kNumTrackeeChunks, trackee_table_->numItems());
  }
  if (getSubprocessId() == SLAVE) {
    IPC::barrier(INIT, 1);
    Transaction slave_transaction_1;
    insert_master_item(&slave_transaction_1);
    EXPECT_TRUE(slave_transaction_1.commit());
    IPC::barrier(TRACKER_DONE, 1);
    IPC::barrier(TRACKER_READ, 1);
    Transaction slave_transaction_2;
    insert_trackees(&slave_transaction_2);
    EXPECT_TRUE(slave_transaction_2.commit());
    IPC::barrier(TRACKEES_DONE, 1);
  }
  IPC::barrier(DIE, 1);
}

TEST_P(NetTableFixture, GetAllIdsNoNewChunkRaceConditionThreads) {
  constexpr size_t kNumPushers = 50;
  constexpr size_t kItemsToPush = 100;

  if (!GetParam()) {
    // No need for separate CR test.
    return;
  }

  auto push_items = [this]() {
    for (size_t i = 0u; i < kItemsToPush; ++i) {
      Transaction transaction;
      Chunk* chunk = table_->newChunk();
      std::shared_ptr<Revision> to_insert = table_->getTemplate();
      common::Id id;
      generateId(&id);
      to_insert->setId(id);
      to_insert->set(kFieldName, 42);
      transaction.insert(table_, chunk, to_insert);
      EXPECT_TRUE(transaction.commit());
    }
  };

  std::thread pushers[kNumPushers];

  for (size_t i = 0u; i < kNumPushers; ++i) {
    pushers[i] = std::thread(push_items);
  }

  std::vector<common::Id> all_ids;
  do {
    Transaction transaction;
    transaction.getAvailableIds(table_, &all_ids);
    for (const common::Id& id : all_ids) {
      ASSERT_TRUE(static_cast<bool>(transaction.getById(id, table_)));
    }
  } while (all_ids.size() < kNumPushers * kItemsToPush);

  for (size_t i = 0u; i < kNumPushers; ++i) {
    pushers[i].join();
  }
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
