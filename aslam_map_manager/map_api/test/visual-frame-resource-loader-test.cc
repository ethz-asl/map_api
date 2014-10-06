#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "./map_api_multiprocess_fixture.h"

#include "map-api/visual-frame-resource-loader.h"

namespace map_api {

class VisualFrameDummy : public common::VisualFrameBase {
  bool releaseResource(const std::string& /*resource_id_hex_string*/) {
    return true;
  }
  bool storeResource(const std::string& /*resource_id_hex_string*/,
                     const cv::Mat& /*resource*/) {
    return true;
  }
};

class ResourceLoaderTest : public MultiprocessTest {
 public:
  enum VisualFrameTableFields {
    kVisualFrameTableUri,
    kVisualFrameTableType,
    kVisualFrameTableVisualFrameId
  };

  virtual void SetUp() override {
    MultiprocessTest::SetUp();
    std::unique_ptr<TableDescriptor> descriptor(new TableDescriptor);
    descriptor->setName(kTableName);
    descriptor->addField<std::string>(kVisualFrameTableUri);
    descriptor->addField<int>(kVisualFrameTableType);
    descriptor->addField<Id>(kVisualFrameTableVisualFrameId);
    NetTableManager::instance().addTable(CRTable::Type::CRU, &descriptor);
    table_ = &NetTableManager::instance().getTable(kTableName);

    Id chunk_id;
    chunk_id.fromHexString(kChunkId);
    Chunk* chunk = table_->newChunk(chunk_id);

    Id visual_frame_id;
    visual_frame_id.fromHexString(kVisualFrameId);

    // generate two dummy entries for DB
    std::shared_ptr<Revision> to_insert_1 = table_->getTemplate();
    Transaction transaction_1;
    Id insert_id_1;
    insert_id_1.fromHexString(kResourceIdA);
    to_insert_1->setId(insert_id_1);
    to_insert_1->set<std::string>(kVisualFrameTableUri,
                                  "test-data/problem.jpg");
    to_insert_1->set<int>(
        kVisualFrameTableType,
        common::ResourceLoaderBase::kVisualFrameResourceRawImageType);
    to_insert_1->set<Id>(kVisualFrameTableVisualFrameId, visual_frame_id);
    transaction_1.insert(table_, chunk, to_insert_1);
    CHECK(transaction_1.commit());

    std::shared_ptr<Revision> to_insert_2 = table_->getTemplate();
    Transaction transaction_2;
    Id insert_id_2;
    insert_id_2.fromHexString(kResourceIdB);
    to_insert_2->setId(insert_id_2);
    to_insert_2->set<std::string>(kVisualFrameTableUri, "test-data/no.png");
    to_insert_2->set<int>(
        kVisualFrameTableType,
        common::ResourceLoaderBase::kVisualFrameResourceRawImageType);
    to_insert_2->set<Id>(kVisualFrameTableVisualFrameId, visual_frame_id);
    transaction_2.insert(table_, chunk, to_insert_2);
    CHECK(transaction_2.commit());
  }

  virtual void TearDown() override { MultiprocessTest::TearDown(); }

  static const std::string kTableName;
  static const std::string kChunkId;
  static const std::string kVisualFrameId;
  static const std::string kResourceIdA;
  static const std::string kResourceIdB;

 private:
  NetTable* table_;
};

const std::string ResourceLoaderTest::kTableName =
    "visual_frame_resource_test_table";

const std::string ResourceLoaderTest::kChunkId =
    "00000000000000000000000000000042";
const std::string ResourceLoaderTest::kVisualFrameId =
    "00000000000000000000000000000001";

const std::string ResourceLoaderTest::kResourceIdA =
    "00000000000000000000000000000007";
const std::string ResourceLoaderTest::kResourceIdB =
    "00000000000000000000000000000008";

TEST_F(ResourceLoaderTest, ShouldFindResourceIds) {
  ResourceLoader loader = ResourceLoader(kTableName);
  std::unordered_set<std::string> resource_ids;
  loader.getResourceIdsOfType(
      ResourceLoaderTest::kVisualFrameId,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      &resource_ids);
  EXPECT_EQ(resource_ids.size(), 2);
  int watchdog = 0;
  for (const auto& id_element : resource_ids) {
    bool isFirst = id_element.compare(kResourceIdA) == 0;
    bool isSecond = id_element.compare(kResourceIdB) == 0;
    EXPECT_TRUE(!isFirst != !isSecond);
    EXPECT_LE(watchdog, 2);
    ++watchdog;
  }
}

TEST_F(ResourceLoaderTest, ShouldLoadResources) {
  ResourceLoader loader = ResourceLoader(kTableName);
  std::shared_ptr<common::VisualFrameBase> dummy_visual_frame(
      new VisualFrameDummy());
  EXPECT_TRUE(loader.loadResource(
      kResourceIdA,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      dummy_visual_frame));
  EXPECT_TRUE(loader.loadResource(
      kResourceIdB,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      dummy_visual_frame));
}
}  // namespace map_api

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
