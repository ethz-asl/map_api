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
  static NetCRTable& instance() {
    static NetCRTable table;
    std::unique_ptr<TableDescriptor> descriptor(new TableDescriptor);
    descriptor->setName("chunk_test_table");
    table.init(&descriptor);
    return table;
  }
};

TEST_F(MultiprocessTest, NetCRInsert) {
  MapApiCore::instance();
  NetCRTable& table = ChunkTestTable::instance();
  std::weak_ptr<Chunk> my_chunk_weak = table.newChunk();
  std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
  EXPECT_TRUE(static_cast<bool>(my_chunk));
  std::shared_ptr<Revision> to_insert = table.getTemplate();
  to_insert->set(CRTable::kIdField, Id::random());
  EXPECT_TRUE(table.insert(my_chunk_weak, to_insert.get()));
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
  NetCRTable& table = ChunkTestTable::instance();
  // the following is a hack until FIXME(tcies) TableManager is instantiated
  if (getSubprocessId() == 0) {
    uint64_t id = launchSubprocess();
    std::weak_ptr<Chunk> my_chunk_weak = table.newChunk();
    std::shared_ptr<Chunk> my_chunk = my_chunk_weak.lock();
    EXPECT_TRUE(static_cast<bool>(my_chunk));

    IPC::barrier(INIT, 1);

    EXPECT_EQ(1, MapApiHub::instance().peerSize());
    EXPECT_EQ(1, my_chunk->requestParticipation());

    IPC::barrier(DIE, 1);

    collectSubprocess(id);
    resetDb();
  } else {
    IPC::barrier(INIT, 1);
    IPC::barrier(DIE, 1);
  }
}

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
