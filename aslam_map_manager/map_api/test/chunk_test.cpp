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
    IPC::init();
    LOG(INFO) << 1;
    if (getSubprocessId() == 0) {
      for (int i = 1; i < nProcesses(); ++i) {
        launchSubprocess();
      }
    }
  }

  virtual void TearDown() {
    if (getSubprocessId() == 0) {
      for (int i = 1; i < nProcesses(); ++i) {
        collectSubprocess(i);
      }
      resetDb();
    }
  }

  void barrier(int barrier_id) {
    IPC::barrier(barrier_id, nProcesses() - 1);
  }

  const std::string kTableName = "chunk_test_table";
  NetCRTable* table_;

 private:
  virtual int nProcesses() = 0;
};

// templating ChunkTest doesn't work, parametrized tests aren't what we want
class ChunkTest1 : public ChunkTest {
  virtual int nProcesses() { return 1; }
};
class ChunkTest2 : public ChunkTest {
  virtual int nProcesses() { return 2; }
};
class ChunkTest3 : public ChunkTest {
  virtual int nProcesses() { return 3; }
};

TEST_F(ChunkTest1, NetCRInsert) {
  std::weak_ptr<Chunk> my_chunk_weak = table_->newChunk();
  std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
  EXPECT_TRUE(static_cast<bool>(my_chunk));
  std::shared_ptr<Revision> to_insert = table_->getTemplate();
  to_insert->set(CRTable::kIdField, Id::random());
  EXPECT_TRUE(table_->insert(my_chunk_weak, to_insert.get()));
}

TEST_F(ChunkTest2, ParticipationRequest) {
  enum Barriers {INIT, DIE};
  if (getSubprocessId() == 0) {
    sleep(1); // TODO(tcies) problem: what if barrier message sent before
    // subprocess initializes handler?
    std::weak_ptr<Chunk> my_chunk_weak = table_->newChunk();
    std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
    EXPECT_TRUE(static_cast<bool>(my_chunk));

    barrier(INIT);

    EXPECT_EQ(1, MapApiHub::instance().peerSize());
    EXPECT_EQ(0, my_chunk->peerSize());
    EXPECT_EQ(1, my_chunk->requestParticipation());
    EXPECT_EQ(1, my_chunk->peerSize());

    barrier(DIE);
  } else {
    barrier(INIT);
    barrier(DIE);
  }
}

TEST_F(ChunkTest3, FullJoinTwice) {
  enum Barriers {INIT, A_JOINED, A_ADDED, B_JOINED, DIE};
  if (getSubprocessId() == 0) {
    sleep(1); // TODO(tcies) problem: what if barrier message sent before
    // subprocess initializes handler?
    std::weak_ptr<Chunk> my_chunk_weak = table_->newChunk();
    std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
    EXPECT_TRUE(static_cast<bool>(my_chunk));

    barrier(INIT);

    EXPECT_EQ(2, MapApiHub::instance().peerSize());
    EXPECT_EQ(0, my_chunk->peerSize());
    EXPECT_EQ(2, my_chunk->requestParticipation());
    EXPECT_EQ(2, my_chunk->peerSize());

    barrier(DIE);
  }
  if(getSubprocessId() == 1){
    barrier(INIT);
    barrier(DIE);
  }
  if(getSubprocessId() == 2){
    barrier(INIT);
    barrier(DIE);
  }
}

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
