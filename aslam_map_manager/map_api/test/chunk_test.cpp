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
    MapApiCore::instance().tableManager().addTable(GetParam(), &descriptor);
    table_ = &MapApiCore::instance().tableManager().getTable(kTableName);
  }

  const std::string kTableName = "chunk_test_table";
  NetTable* table_;
};

TEST_P(ChunkTest, NetInsert) {
  std::weak_ptr<Chunk> my_chunk_weak = table_->newChunk();
  std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
  EXPECT_TRUE(static_cast<bool>(my_chunk));
  std::shared_ptr<Revision> to_insert = table_->getTemplate();
  to_insert->set(CRTable::kIdField, Id::random());
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
    EXPECT_TRUE(table_->insert(table_->getChunk(chunk_id), to_insert.get()));

    IPC::barrier(A_ADDED, 1);
    IPC::barrier(DIE, 1);
  }
}

INSTANTIATE_TEST_CASE_P(Default, ChunkTest, ::testing::Values(true, false));

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
