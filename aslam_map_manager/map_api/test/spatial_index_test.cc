#include <string>

#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "map-api/core.h"
#include "map-api/net-table.h"
#include "map-api/net-table-transaction.h"
#include "map-api/transaction.h"

#include "./map_api_multiprocess_fixture.h"

namespace map_api {

/**
 *
 * Predefined bounding boxes a, b, c, d:
 *
 *  x=0           x=2       z=0       z=2
 *   _______________  _ _ _  ___________  y=0
 *  |  ___  |       |       |___  |     |
 *  | | a | |       |       | a | |     |
 *  | |___|_|_      |       |___|_|_    |
 *  |_|_b_|_|d|_____|       |_b_|_|d|___|
 *  | |___|_|_|     |       |___|_|_|   |
 *  |       |       |       |     |     |
 *  |       |       |       |     |     |
 *  |_______|_______| _ _ _ |_____|_____| y=2
 *
 *  |               |
 *   _______________  z=0
 *  | | a | |c|     |
 *  | |___|_|_|     |
 *  |_____|_|d|_____|
 *  |     |_|_|     |
 *  |       |       |
 *  |_______|_______| z=2
 *
 * (0, 0, 0) has {a, b, c, d}
 * (0, 1, 0) has {b, c, d}
 * (1, *, 0) has {c, d}
 * (*, *, 1) has {d}
 */
class SpatialIndexTest : public MultiprocessTest {
 protected:
  static const std::string kTableName;
  enum Fields {
    kFieldName
  };

  static constexpr double kBounds[] = {0, 2, 0, 2, 0, 2};
  static constexpr double kABox[] = {0.25, 0.75, 0.25, 0.75, 0, 0.6};
  static constexpr double kBBox[] = {0.25, 0.75, 0.75, 1, 25, 0, 0.6};
  static constexpr double kCBox[] = {0.75, 1.25, 0.75, 1, 25, 0, 0.6};
  static constexpr double kDBox[] = {0.75, 1.25, 0.75, 1, 25, 0.6, 1.3};
  static constexpr size_t kSubdiv[] = {2u, 2u, 2u};

  static inline SpatialIndex::BoundingBox box(const double* box) {
    SpatialIndex::BoundingBox out;
    out.push_back({box[0], box[1]});
    out.push_back({box[2], box[3]});
    out.push_back({box[4], box[5]});
    return out;
  }

  virtual void SetUp() {
    MultiprocessTest::SetUp();
    std::unique_ptr<TableDescriptor> descriptor(new TableDescriptor);
    descriptor->setName(kTableName);
    descriptor->addField<int>(kFieldName);
    NetTableManager::instance().addTable(CRTable::Type::CR, &descriptor);
    table_ = &NetTableManager::instance().getTable(kTableName);
  }

  void createSpatialIndex() {
    table_->createSpatialIndex(box(kBounds),
                               std::vector<size_t>(kSubdiv, kSubdiv + 3));
  }

  void joinSpatialIndex(const PeerId& entry_point) {
    table_->joinSpatialIndex(
        box(kBounds), std::vector<size_t>(kSubdiv, kSubdiv + 3), entry_point);
  }

  void createDefaultChunks() {}

  NetTable* table_;
  Chunk* chunk_a_, chunk_b_, chunk_c_, chunk_d_;
};

}  // namespace map_api

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
