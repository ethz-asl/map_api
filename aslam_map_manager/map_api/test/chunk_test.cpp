#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "map-api/chunk-manager.h"
#include "map-api/ipc.h"
#include "map-api/map-api-core.h"
#include "map-api/net-cr-table.h"

#include "multiprocess_fixture.cpp"

using namespace map_api;

class ChunkTestTable : public NetCRTable {
 public:
  virtual const std::string name() const final override {
    return "chunk_test_table";
  }
  virtual void defineFieldsNetCRDerived() final override {

  }
  MEYERS_SINGLETON_INSTANCE_FUNCTION_DIRECT(ChunkTestTable);
 protected:
  MAP_API_TABLE_SINGLETON_PATTERN_PROTECTED_METHODS_DIRECT(ChunkTestTable);
};

TEST_F(MultiprocessTest, NetCRInsert) {
  MapApiCore::instance();
  ChunkTestTable& table = ChunkTestTable::instance();
  table.init();
  std::weak_ptr<Chunk> my_chunk_weak =
      ChunkManager::instance().newChunk(table);
  std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
  EXPECT_TRUE(static_cast<bool>(my_chunk));
  std::shared_ptr<Revision> to_insert = table.getTemplate();
  EXPECT_TRUE(table.netInsert(my_chunk_weak, to_insert.get()));
  resetDb();
}

/**
 * TODO(tcies) verify chunk confirms peer, not just whether peer Acks the
 * participation request
 */
TEST_F(MultiprocessTest, ParticipationRequest) {
  enum Barriers {INIT, DIE};
  IPC::init();
  MapApiCore::instance();
  ChunkTestTable& table = ChunkTestTable::instance();
  table.init();
  CRTable* raw_cr_table = dynamic_cast<CRTable*>(&table);
  ASSERT_TRUE(static_cast<bool>(raw_cr_table));
  // the following is a hack until FIXME(tcies) TableManager is instantiated
  ChunkManager::instance().init(raw_cr_table);
  if (getSubprocessId() == 0) {
    uint64_t id = launchSubprocess();
    std::weak_ptr<Chunk> my_chunk_weak =
        ChunkManager::instance().newChunk(table);
    std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
    EXPECT_TRUE(static_cast<bool>(my_chunk));

    IPC::barrier(INIT, 1);

    EXPECT_EQ(1, MapApiHub::instance().peerSize());
    EXPECT_EQ(1, ChunkManager::instance().requestParticipation(*my_chunk));

    IPC::barrier(DIE, 1);

    collectSubprocess(id);
    resetDb();
  } else {
    IPC::barrier(INIT, 1);
    IPC::barrier(DIE, 1);
  }
}

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
