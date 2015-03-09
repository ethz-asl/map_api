#include <string>

#include <eigen-checks/gtest.h>

#include "map-api/core.h"
#include "map-api/ipc.h"
#include "map-api/net-table.h"
#include "map-api/net-table-manager.h"
#include "map-api/net-table-transaction.h"
#include "map-api/test/testing-entrypoint.h"
#include "map-api/transaction.h"
#include "./map_api_fixture.h"

namespace map_api {

/**
 *
 * Predefined bounding boxes a, b, c, d:
 *
 *  x=0           x=2       z=0       z=2
 *   _______________  _ _ _  ___________  y=0
 *  |  ___ _|_      |       |___  |     |
 *  | | a | |c|     |       | a | |     |
 *  | |___|_|_|___  |       |___|_|_ ___|
 *  |_____|_|b|_d_|_| _ _ _ |___|_|b|_d_|
 *  |     |_|_|___| |       |   |_|_|___|
 *  |       |       |       |     |     |
 *  |       |       |       |     |     |
 *  |_______|_______| _ _ _ |_____|_____| y=2
 *
 *  |       |       |
 *   _______________  z=0
 *  | | a | |c|     |
 *  | |___|_|_|     |
 *  |_____|_|b|_____|
 *  |     |_|_|___  |
 *  |       | | d | |
 *  |_______|_|___|_| z=2
 *
 * (0, 0, 0) has {a, b, c}
 * (0, 0, 1) has {b}
 * (0, 1, 0) has {b}
 * (0, 1, 1) has {b}
 * (1, 0, 0) has {b, c}
 * (1, 0, 1) has {b, d}
 * (1, 1, 0) has {b}
 * (1, 1, 1) has {b, d}
 */
class SpatialIndexTest : public MapApiFixture {
 protected:
  static const std::string kTableName;
  enum Fields {
    kFieldName
  };

  static const double kBounds[];
  static const double kABox[];
  static const double kBBox[];
  static const double kCBox[];
  static const double kDBox[];
  static const size_t kSubdiv[];

  static inline SpatialIndex::BoundingBox box(const double* box) {
    SpatialIndex::BoundingBox out;
    out.push_back({box[0], box[1]});
    out.push_back({box[2], box[3]});
    out.push_back({box[4], box[5]});
    return out;
  }

  virtual void SetUp() {
    MultiprocessFixture::SetUp();
    std::unique_ptr<TableDescriptor> descriptor(new TableDescriptor);
    descriptor->setName(kTableName);
    descriptor->addField<int>(kFieldName);
    table_ =
        NetTableManager::instance().addTable(CRTable::Type::CR, &descriptor);

    generateIdFromInt(1, &chunk_a_id_);
    generateIdFromInt(2, &chunk_b_id_);
    generateIdFromInt(3, &chunk_c_id_);
    generateIdFromInt(4, &chunk_d_id_);

    expected_a_.insert(chunk_a_id_);
    expected_a_.insert(chunk_b_id_);
    expected_a_.insert(chunk_c_id_);

    expected_b_.insert(chunk_a_id_);
    expected_b_.insert(chunk_b_id_);
    expected_b_.insert(chunk_c_id_);
    expected_b_.insert(chunk_d_id_);

    expected_c_.insert(chunk_a_id_);
    expected_c_.insert(chunk_b_id_);
    expected_c_.insert(chunk_c_id_);

    expected_d_.insert(chunk_b_id_);
    expected_d_.insert(chunk_d_id_);
  }

  void createSpatialIndex() {
    table_->createSpatialIndex(box(kBounds),
                               std::vector<size_t>(kSubdiv, kSubdiv + 3));
  }

  void joinSpatialIndex(const PeerId& entry_point) {
    table_->joinSpatialIndex(
        box(kBounds), std::vector<size_t>(kSubdiv, kSubdiv + 3), entry_point);
  }

  void createDefaultChunks() {
    chunk_a_ = table_->newChunk(chunk_a_id_);
    chunk_b_ = table_->newChunk(chunk_b_id_);
    chunk_c_ = table_->newChunk(chunk_c_id_);
    chunk_d_ = table_->newChunk(chunk_d_id_);
  }

  void registerDefaultChunks() {
    table_->registerChunkInSpace(chunk_a_id_, box(kABox));
    table_->registerChunkInSpace(chunk_b_id_, box(kBBox));
    table_->registerChunkInSpace(chunk_c_id_, box(kCBox));
    table_->registerChunkInSpace(chunk_d_id_, box(kDBox));
  }

  bool checkExpectedActive(
      const std::unordered_set<common::Id>& expected_set) const {
    std::set<common::Id> active_set;
    table_->getActiveChunkIds(&active_set);
    std::unordered_set<common::Id> active(active_set.begin(), active_set.end());
    return expected_set == active;
  }

  NetTable* table_;
  Chunk* chunk_a_, *chunk_b_, *chunk_c_, *chunk_d_;
  common::Id chunk_a_id_, chunk_b_id_, chunk_c_id_, chunk_d_id_;
  std::unordered_set<common::Id> expected_a_, expected_b_, expected_c_,
      expected_d_;
};

const std::string SpatialIndexTest::kTableName = "table";
const double SpatialIndexTest::kBounds[] = {0, 2, 0, 2, 0, 2};
const double SpatialIndexTest::kABox[] = {0.25, 0.75, 0.25, 0.75, 0, 0.75};
const double SpatialIndexTest::kBBox[] = {0.75, 1.25, 0.75, 1.25, 0.75, 1.25};
const double SpatialIndexTest::kCBox[] = {0.25, 0.75, 0.75, 1.25, 0, 0.75};
const double SpatialIndexTest::kDBox[] = {1.25, 1.75, 0.75, 1.25, 1.25, 1.99};
const size_t SpatialIndexTest::kSubdiv[] = {2u, 2u, 2u};

TEST_F(SpatialIndexTest, CellDimensions) {
  createSpatialIndex();
  const Eigen::AlignedBox3d kExpected[] = {
      Eigen::AlignedBox3d(Eigen::Vector3d(0, 0, 0), Eigen::Vector3d(1, 1, 1)),
      Eigen::AlignedBox3d(Eigen::Vector3d(0, 0, 1), Eigen::Vector3d(1, 1, 2)),
      Eigen::AlignedBox3d(Eigen::Vector3d(0, 1, 0), Eigen::Vector3d(1, 2, 1)),
      Eigen::AlignedBox3d(Eigen::Vector3d(0, 1, 1), Eigen::Vector3d(1, 2, 2)),
      Eigen::AlignedBox3d(Eigen::Vector3d(1, 0, 0), Eigen::Vector3d(2, 1, 1)),
      Eigen::AlignedBox3d(Eigen::Vector3d(1, 0, 1), Eigen::Vector3d(2, 1, 2)),
      Eigen::AlignedBox3d(Eigen::Vector3d(1, 1, 0), Eigen::Vector3d(2, 2, 1)),
      Eigen::AlignedBox3d(Eigen::Vector3d(1, 1, 1), Eigen::Vector3d(2, 2, 2))};
  int pos = 0;
  EXPECT_EQ(8u, table_->spatial_index().size());
  for (SpatialIndex::Cell& cell : table_->spatial_index()) {
    Eigen::AlignedBox3d cell_box;
    cell.getDimensions(&cell_box);
    EXPECT_TRUE(
        EIGEN_MATRIX_EQUAL_DOUBLE(cell_box.min(), kExpected[pos].min()));
    EXPECT_TRUE(
        EIGEN_MATRIX_EQUAL_DOUBLE(cell_box.max(), kExpected[pos].max()));
    ++pos;
  }
}

TEST_F(SpatialIndexTest, RegisterSeek) {
  enum Barriers {
    INIT,
    PUSH_ADDRESS,
    SPATIAL_REGISTERED,
    DIE
  };
  enum Processes {
    ROOT,
    A
  };
  if (getSubprocessId() == ROOT) {
    createSpatialIndex();
    launchSubprocess(A);
    IPC::barrier(INIT, 1);
    IPC::push(PeerId::self());
    IPC::barrier(PUSH_ADDRESS, 1);
    IPC::barrier(SPATIAL_REGISTERED, 1);

    std::unordered_set<Chunk*> result;
    table_->getChunksInBoundingBox(box(kABox), &result);
    EXPECT_TRUE(checkExpectedActive(expected_a_));
    table_->leaveAllChunks();
    table_->getChunksInBoundingBox(box(kBBox), &result);
    EXPECT_TRUE(checkExpectedActive(expected_b_));
    table_->leaveAllChunks();
    table_->getChunksInBoundingBox(box(kCBox), &result);
    EXPECT_TRUE(checkExpectedActive(expected_c_));
    table_->leaveAllChunks();
    table_->getChunksInBoundingBox(box(kDBox), &result);
    EXPECT_TRUE(checkExpectedActive(expected_d_));
    table_->leaveAllChunks();

    IPC::barrier(DIE, 1);
  }
  if (getSubprocessId() == A) {
    IPC::barrier(INIT, 1);
    IPC::barrier(PUSH_ADDRESS, 1);
    PeerId root = IPC::pop<PeerId>();
    joinSpatialIndex(root);
    createDefaultChunks();
    registerDefaultChunks();
    IPC::barrier(SPATIAL_REGISTERED, 1);
    IPC::barrier(DIE, 1);
  }
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
