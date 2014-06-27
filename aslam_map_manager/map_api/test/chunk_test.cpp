#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "map-api/ipc.h"
#include "map-api/map-api-core.h"
#include "map-api/net-table.h"

#include "multiprocess_fixture.cpp"

using namespace map_api;

class ChunkTest : public MultiprocessTest,
public ::testing::WithParamInterface<bool> {
 protected:
  virtual void SetUp() {
    MultiprocessTest::SetUp();
    std::unique_ptr<TableDescriptor> descriptor(new TableDescriptor);
    descriptor->setName(kTableName);
    descriptor->addField<int>(kFieldName);
    MapApiCore::instance().tableManager().addTable(GetParam(), &descriptor);
    table_ = &MapApiCore::instance().tableManager().getTable(kTableName);
  }

  const std::string kTableName = "chunk_test_table";
  const std::string kFieldName = "chunk_test_field";
  NetTable* table_;
};

TEST_P(ChunkTest, NetInsert) {
  std::weak_ptr<Chunk> my_chunk_weak = table_->newChunk();
  std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
  EXPECT_TRUE(static_cast<bool>(my_chunk));
  std::shared_ptr<Revision> to_insert = table_->getTemplate();
  to_insert->set(CRTable::kIdField, Id::random());
  to_insert->set(kFieldName, 42);
  EXPECT_TRUE(table_->insert(my_chunk_weak, to_insert.get()));
}

TEST_P(ChunkTest, ParticipationRequest) {
  enum SubProcesses {ROOT, A};
  enum Barriers {INIT, DIE};
  if (getSubprocessId() == ROOT) {
    launchSubprocess(A);
    std::weak_ptr<Chunk> my_chunk_weak = table_->newChunk();
    std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
    EXPECT_TRUE(static_cast<bool>(my_chunk));

    IPC::barrier(INIT, 1);

    EXPECT_EQ(1, MapApiHub::instance().peerSize());
    EXPECT_EQ(0, my_chunk->peerSize());
    EXPECT_EQ(1, my_chunk->requestParticipation());
    EXPECT_EQ(1, my_chunk->peerSize());

    IPC::barrier(DIE, 1);
  } else {
    IPC::barrier(INIT, 1);
    IPC::barrier(DIE, 1);
  }
}

TEST_P(ChunkTest, FullJoinTwice) {
  enum SubProcesses {ROOT, A, B};
  enum Barriers {ROOT_A_INIT, A_JOINED_B_INIT, B_JOINED, DIE};
  if (getSubprocessId() == ROOT) {
    launchSubprocess(A);
    std::weak_ptr<Chunk> my_chunk_weak = table_->newChunk();
    std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
    EXPECT_TRUE(static_cast<bool>(my_chunk));
    std::shared_ptr<Revision> to_insert = table_->getTemplate();
    to_insert->set(CRTable::kIdField, Id::random());
    to_insert->set(kFieldName, 42);
    EXPECT_TRUE(table_->insert(my_chunk_weak, to_insert.get()));

    IPC::barrier(ROOT_A_INIT, 1);

    EXPECT_EQ(1, MapApiHub::instance().peerSize());
    EXPECT_EQ(0, my_chunk->peerSize());
    EXPECT_EQ(1, my_chunk->requestParticipation());
    EXPECT_EQ(1, my_chunk->peerSize());
    launchSubprocess(B);

    IPC::barrier(A_JOINED_B_INIT, 2);

    EXPECT_EQ(2, MapApiHub::instance().peerSize());
    EXPECT_EQ(1, my_chunk->peerSize());
    EXPECT_EQ(1, my_chunk->requestParticipation());
    EXPECT_EQ(2, my_chunk->peerSize());

    IPC::barrier(B_JOINED, 2);

    IPC::barrier(DIE, 2);
  }
  if(getSubprocessId() == A){
    IPC::barrier(ROOT_A_INIT, 1);
    IPC::barrier(A_JOINED_B_INIT, 2);
    std::unordered_map<Id, std::shared_ptr<Revision> > dummy;
    table_->dumpCache(Time::now(), &dummy);
    EXPECT_EQ(1, dummy.size());
    IPC::barrier(B_JOINED, 2);
    IPC::barrier(DIE, 2);
  }
  if(getSubprocessId() == B){
    IPC::barrier(A_JOINED_B_INIT, 2);
    IPC::barrier(B_JOINED, 2);
    std::unordered_map<Id, std::shared_ptr<Revision> > dummy;
    table_->dumpCache(Time::now(), &dummy);
    EXPECT_EQ(1, dummy.size());
    IPC::barrier(DIE, 2);
  }
}

TEST_P(ChunkTest, RemoteInsert) {
  enum Subprocesses {ROOT, A};
  enum Barriers {INIT, A_JOINED, A_ADDED, DIE};
  if (getSubprocessId() == ROOT) {
    launchSubprocess(A);
    std::weak_ptr<Chunk> my_chunk_weak = table_->newChunk();
    std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
    EXPECT_TRUE(static_cast<bool>(my_chunk));
    IPC::barrier(INIT, 1);

    my_chunk->requestParticipation();
    IPC::push(my_chunk->id().hexString());
    IPC::barrier(A_JOINED, 1);
    IPC::barrier(A_ADDED, 1);

    std::unordered_map<Id, std::shared_ptr<Revision> > dummy;
    table_->dumpCache(Time::now(), &dummy);
    EXPECT_EQ(1, dummy.size());
    IPC::barrier(DIE, 1);
  }
  if (getSubprocessId() == A) {
    IPC::barrier(INIT, 1);
    IPC::barrier(A_JOINED, 1);
    std::string chunk_id_string;
    Id chunk_id;
    IPC::pop(&chunk_id_string);
    chunk_id.fromHexString(chunk_id_string);
    std::shared_ptr<Revision> to_insert = table_->getTemplate();
    to_insert->set(CRTable::kIdField, Id::random());
    to_insert->set(kFieldName, 42);
    EXPECT_TRUE(table_->insert(table_->getChunk(chunk_id), to_insert.get()));

    IPC::barrier(A_ADDED, 1);
    IPC::barrier(DIE, 1);
  }
}

TEST_P(ChunkTest, RemoteUpdate) {
  if (!GetParam()) { // not updateable - just pass test
    return;
  }
  enum Subprocesses {ROOT, A};
  enum Barriers {INIT, A_JOINED, A_UPDATED, DIE};
  std::unordered_map<Id, std::shared_ptr<Revision> > results;
  if (getSubprocessId() == ROOT) {
    launchSubprocess(A);
    std::weak_ptr<Chunk> my_chunk_weak = table_->newChunk();
    std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
    EXPECT_TRUE(static_cast<bool>(my_chunk));
    std::shared_ptr<Revision> to_insert = table_->getTemplate();
    to_insert->set(CRTable::kIdField, Id::random());
    to_insert->set(kFieldName, 42);
    EXPECT_TRUE(table_->insert(my_chunk_weak, to_insert.get()));
    table_->dumpCache(Time::now(), &results);
    EXPECT_EQ(1, results.size());
    EXPECT_TRUE(results.begin()->second->verify(kFieldName, 42));
    IPC::barrier(INIT, 1);

    my_chunk->requestParticipation();
    IPC::barrier(A_JOINED, 1);
    IPC::barrier(A_UPDATED, 1);
    table_->dumpCache(Time::now(), &results);
    EXPECT_EQ(1, results.size());
    EXPECT_TRUE(results.begin()->second->verify(kFieldName, 21));

    IPC::barrier(DIE, 1);
  }
  if (getSubprocessId() == A) {
    IPC::barrier(INIT, 1);
    IPC::barrier(A_JOINED, 1);
    table_->dumpCache(Time::now(), &results);
    EXPECT_EQ(1, results.size());
    results.begin()->second->set(kFieldName, 21);
    EXPECT_TRUE(table_->update(results.begin()->second.get()));

    IPC::barrier(A_UPDATED, 1);
    IPC::barrier(DIE, 1);
  }
}

DEFINE_uint64(grind_processes, 10u,
              "Total amount of processes in ChunkTest.Grind");
DEFINE_uint64(grind_cycles, 10u,
              "Total amount of insert-update cycles in ChunkTest.Grind");

TEST_P(ChunkTest, Grind) {
  const int kInsertUpdateCycles = FLAGS_grind_cycles;
  const uint64_t kProcesses = FLAGS_grind_processes;
  enum Barriers {INIT, ID_SHARED, DIE};
  std::unordered_map<Id, std::shared_ptr<Revision> > results;
  if (getSubprocessId() == 0) {
    std::ostringstream extra_flags_ss;
    extra_flags_ss << "--grind_processes=" << FLAGS_grind_processes << " ";
    extra_flags_ss << "--grind_cycles=" << FLAGS_grind_cycles;
    for (uint64_t i = 1u; i < kProcesses; ++i) {
      launchSubprocess(i, extra_flags_ss.str());
    }
    std::weak_ptr<Chunk> my_chunk_weak = table_->newChunk();
    std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
    EXPECT_TRUE(static_cast<bool>(my_chunk));
    IPC::barrier(INIT, kProcesses - 1);
    my_chunk->requestParticipation();
    IPC::push(my_chunk->id().hexString());
    IPC::barrier(ID_SHARED, kProcesses - 1);
    IPC::barrier(DIE, kProcesses - 1);
    table_->dumpCache(Time::now(), &results);
    EXPECT_EQ(kInsertUpdateCycles * (kProcesses - 1), results.size());
  } else {
    IPC::barrier(INIT, kProcesses - 1);
    IPC::barrier(ID_SHARED, kProcesses - 1);
    std::string chunk_id_string;
    Id chunk_id;
    IPC::pop(&chunk_id_string);
    chunk_id.fromHexString(chunk_id_string);
    std::weak_ptr<Chunk> my_chunk_weak = table_->getChunk(chunk_id);
    for (int i = 0; i < kInsertUpdateCycles; ++i) {
      // insert
      Id insert_id; // random ID doesn't work!!! TODO(tcies) fix or abandon
      std::ostringstream id_ss("00000000000000000000000000000000");
      id_ss << getSubprocessId() << "a" << i << "a";
      insert_id.fromHexString(id_ss.str());
      std::shared_ptr<Revision> to_insert = table_->getTemplate();
      to_insert->set(CRTable::kIdField, insert_id);
      to_insert->set(kFieldName, 42);
      EXPECT_TRUE(table_->insert(my_chunk_weak, to_insert.get()));
      // update
      if (GetParam()){
        table_->dumpCache(Time::now(), &results);
        results.begin()->second->set(kFieldName, 21);
        EXPECT_TRUE(table_->update(results.begin()->second.get()));
      }
    }
    IPC::barrier(DIE, kProcesses - 1);
  }
}

TEST_P(ChunkTest, Transactions) {
  const uint64_t kProcesses = FLAGS_grind_processes;
  enum Barriers {INIT, IDS_SHARED, DIE};
  std::unordered_map<Id, std::shared_ptr<Revision> > results;
  if (getSubprocessId() == 0) {
    std::ostringstream extra_flags_ss;
    extra_flags_ss << "--grind_processes=" << FLAGS_grind_processes << " ";
    for (uint64_t i = 1u; i < kProcesses; ++i) {
      launchSubprocess(i, extra_flags_ss.str());
    }
    std::weak_ptr<Chunk> my_chunk_weak = table_->newChunk();
    std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
    EXPECT_TRUE(static_cast<bool>(my_chunk));
    Id insert_id = Id::random();
    std::shared_ptr<Revision> to_insert = table_->getTemplate();
    to_insert->set(CRTable::kIdField, insert_id);
    to_insert->set(kFieldName, 1);
    EXPECT_TRUE(table_->insert(my_chunk_weak, to_insert.get()));
    IPC::barrier(INIT, kProcesses - 1);

    my_chunk->requestParticipation();
    IPC::push(my_chunk->id().hexString());
    IPC::push(insert_id.hexString());
    IPC::barrier(IDS_SHARED, kProcesses - 1);

    IPC::barrier(DIE, kProcesses - 1);
    table_->dumpCache(Time::now(), &results);
    EXPECT_EQ(kProcesses, results.size());
    std::unordered_map<Id, std::shared_ptr<Revision> >::iterator found =
        results.find(insert_id);
    if (found != results.end()) {
      int final_value;
      found->second->get(kFieldName, &final_value);
      if (GetParam()){
        EXPECT_EQ(kProcesses, final_value);
      } else {
        EXPECT_EQ(1, final_value);
      }
    } else {
      // still need a clean disconnect
      EXPECT_TRUE(false);
    }
  } else {
    IPC::barrier(INIT, kProcesses - 1);
    IPC::barrier(IDS_SHARED, kProcesses - 1);
    std::string chunk_id_string, item_id_string;
    Id chunk_id, item_id;
    IPC::pop(&chunk_id_string);
    IPC::pop(&item_id_string);
    chunk_id.fromHexString(chunk_id_string);
    item_id.fromHexString(item_id_string);
    std::weak_ptr<Chunk> my_chunk_weak = table_->getChunk(chunk_id);
    std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
    EXPECT_TRUE(static_cast<bool>(my_chunk));
    std::shared_ptr<ChunkTransaction> transaction;
    while (true) {
      transaction = my_chunk->newTransaction();
      // insert
      Id insert_id; // random ID doesn't work!!! TODO(tcies) fix or abandon
      std::ostringstream id_ss("00000000000000000000000000000000");
      id_ss << getSubprocessId() << "a";
      insert_id.fromHexString(id_ss.str());
      std::shared_ptr<Revision> to_insert = table_->getTemplate();
      to_insert->set(CRTable::kIdField, insert_id);
      to_insert->set(kFieldName, 42);
      transaction->insert(to_insert);
      // update
      if (GetParam()){
        int transient_value;
        std::shared_ptr<Revision> to_update = transaction->getById(item_id);
        to_update->get(kFieldName, &transient_value);
        ++transient_value;
        to_update->set(kFieldName, transient_value);
        transaction->update(to_update);
      }
      if (my_chunk->commit(*transaction)){
        break;
      }
    }
    IPC::barrier(DIE, kProcesses - 1);
  }
}

INSTANTIATE_TEST_CASE_P(Default, ChunkTest, ::testing::Values(false, true));

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
