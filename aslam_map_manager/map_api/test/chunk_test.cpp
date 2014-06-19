#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "map-api/chunk-manager.h"
#include "map-api/ipc.h"
#include "map-api/map-api-core.h"
#include "map-api/net-cr-table.h"

#include "multiprocess_fixture.cpp"

using namespace map_api;

class ChunkTest : public MultiprocessTest {
 protected:
  virtual void SetUp() {
    MultiprocessTest::SetUp();
    MapApiCore::instance().tableManager().clear();
    std::unique_ptr<TableDescriptor> descriptor(new TableDescriptor);
    descriptor->setName(kTableName);
    MapApiCore::instance().tableManager().addTable(&descriptor);
    table_ = &MapApiCore::instance().tableManager().getTable(kTableName);
  }

  const std::string kTableName = "chunk_test_table";
  NetCRTable* table_;
};

TEST_F(ChunkTest, NetCRInsert) {
  std::weak_ptr<Chunk> my_chunk_weak = table_->newChunk();
  std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
  EXPECT_TRUE(static_cast<bool>(my_chunk));
  std::shared_ptr<Revision> to_insert = table_->getTemplate();
  to_insert->set(CRTable::kIdField, Id::random());
  EXPECT_TRUE(table_->insert(my_chunk_weak, to_insert.get()));
}

TEST_F(ChunkTest, ParticipationRequest) {
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

    LOG(INFO) << "oi";
    IPC::barrier(DIE, 1);
    LOG(INFO) << "oi";
  } else {
    IPC::barrier(INIT, 1);
    IPC::barrier(DIE, 1);
  }
}

TEST_F(ChunkTest, FullJoinTwice) {
  enum SubProcesses {ROOT, A, B};
  enum Barriers {ROOT_A_INIT, A_JOINED, A_ADDED_B_INIT, B_JOINED, DIE};
  if (getSubprocessId() == ROOT) {
    launchSubprocess(A);
    std::weak_ptr<Chunk> my_chunk_weak = table_->newChunk();
    std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
    EXPECT_TRUE(static_cast<bool>(my_chunk));

    IPC::barrier(ROOT_A_INIT, 1);

    EXPECT_EQ(1, MapApiHub::instance().peerSize());
    EXPECT_EQ(0, my_chunk->peerSize());
    EXPECT_EQ(1, my_chunk->requestParticipation());
    EXPECT_EQ(1, my_chunk->peerSize());

    IPC::barrier(A_JOINED, 1);

    launchSubprocess(B);

    IPC::barrier(A_ADDED_B_INIT, 2);

    EXPECT_EQ(2, MapApiHub::instance().peerSize());
    EXPECT_EQ(1, my_chunk->peerSize());
    EXPECT_EQ(1, my_chunk->requestParticipation());
    EXPECT_EQ(2, my_chunk->peerSize());

    IPC::barrier(B_JOINED, 2);

    IPC::barrier(DIE, 2);
  }
  if(getSubprocessId() == A){
    IPC::barrier(ROOT_A_INIT, 1);
    IPC::barrier(A_JOINED, 1);
    IPC::barrier(A_ADDED_B_INIT, 2);
    IPC::barrier(B_JOINED, 2);
    IPC::barrier(DIE, 2);
  }
  if(getSubprocessId() == B){
    IPC::barrier(A_ADDED_B_INIT, 2);
    IPC::barrier(B_JOINED, 2);
    IPC::barrier(DIE, 2);
  }
}

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
