#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "./map_api_multiprocess_fixture.h"

#include "map-api/visual-frame-resource-loader.h"

namespace map_api {

class VisualFrameDummy : public common::VisualFrameBase {
  bool releaseResource(std::string) { return true; }

  bool storeResource(std::string resourceId, cv::Mat resource) {
    // TODO(mfehr): REMOVE THIS
    std::cout << "stored resource in visual frame" << std::endl;
    cv::namedWindow(resourceId, cv::WINDOW_AUTOSIZE);
    cv::imshow(resourceId, resource);
    cv::waitKey(5000);
    return true;
  }
};

class ResourceLoaderTest : public MultiprocessTest {
 public:
  virtual void SetUp() override {
    MultiprocessTest::SetUp();
    std::unique_ptr<TableDescriptor> descriptor(new TableDescriptor);
    // TODO(mfehr): table name and field names are defined in app.h, move to
    // some place we can reach from here
    descriptor->setName("visual_inertial_mapping_visual_frame_table");
    descriptor->addField<std::string>(0);
    descriptor->addField<int>(1);
    descriptor->addField<map_api::Id>(2);
    NetTableManager::instance().addTable(CRTable::Type::CRU, &descriptor);
    table_ = &NetTableManager::instance().getTable(
                  "visual_inertial_mapping_visual_frame_table");

    Id chunk_id;
    chunk_id.fromHexString(kChunkId);
    Chunk* chunk = table_->newChunk(chunk_id);

    Id visual_frame_id;
    visual_frame_id.fromHexString(kVisualFrameId);

    // generate two dummy entries for DB

    /*
    enum VisualFrameTableFields {
     kVisualFrameTableUri,
     kVisualFrameTableType,
     kVisualFrameTableVisualFrameId
    };
    */

    std::shared_ptr<Revision> to_insert_1 = table_->getTemplate();
    Transaction transaction_1;
    Id insert_id_1;
    insert_id_1.fromHexString(kResourceIdA);
    to_insert_1->setId(insert_id_1);
    to_insert_1->set<std::string>(0, "test-data/problem.jpg");
    to_insert_1->set<int>(
        1, common::ResourceLoaderBase::kVisualFrameResourceRawImageType);
    to_insert_1->set<Id>(2, visual_frame_id);
    transaction_1.insert(table_, chunk, to_insert_1);
    CHECK(transaction_1.commit());

    std::shared_ptr<Revision> to_insert_2 = table_->getTemplate();
    Transaction transaction_2;
    Id insert_id_2;
    insert_id_2.fromHexString(kResourceIdB);
    to_insert_2->setId(insert_id_2);
    to_insert_2->set<std::string>(0, "test-data/no.png");
    to_insert_2->set<int>(
        1, common::ResourceLoaderBase::kVisualFrameResourceRawImageType);
    to_insert_2->set<Id>(2, visual_frame_id);
    transaction_2.insert(table_, chunk, to_insert_2);
    CHECK(transaction_2.commit());
  }

  virtual void TearDown() override { MultiprocessTest::TearDown(); }

  static const std::string kChunkId;
  static const std::string kVisualFrameId;
  static const std::string kResourceIdA;
  static const std::string kResourceIdB;

 private:
  NetTable* table_;
};

const std::string ResourceLoaderTest::kChunkId =
    "00000000000000000000000000000042";
const std::string ResourceLoaderTest::kVisualFrameId =
    "00000000000000000000000000000001";

const std::string ResourceLoaderTest::kResourceIdA =
    "00000000000000000000000000000007";
const std::string ResourceLoaderTest::kResourceIdB =
    "00000000000000000000000000000008";

TEST_F(ResourceLoaderTest, ShouldFindResourceIds) {
  // TODO(mfehr): make sure tests are independent, ? TearDown() ?
  ResourceLoader loader = ResourceLoader();
  std::list<std::string> resource_ids = std::list<std::string>();
  loader.getResourceIds(
      ResourceLoaderTest::kVisualFrameId,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      resource_ids);
  EXPECT_EQ(resource_ids.size(), 2);
  int watchdog = 0;
  for (std::list<std::string>::const_iterator iterator = resource_ids.begin(),
                                              end = resource_ids.end();
       iterator != end; ++iterator) {
    bool isFirst = iterator->compare(kResourceIdA) == 0;
    bool isSecond = iterator->compare(kResourceIdB) == 0;
    EXPECT_TRUE(!isFirst != !isSecond);
    EXPECT_LE(watchdog, 2);
    ++watchdog;
  }
}

TEST_F(ResourceLoaderTest, ShouldLoadResources) {
  // TODO(mfehr): make sure tests are independent, ? TearDown() ?
  ResourceLoader loader = ResourceLoader();
  std::shared_ptr<common::VisualFrameBase> visual_frame_ptr =
      std::shared_ptr<common::VisualFrameBase>(new VisualFrameDummy());
  EXPECT_TRUE(loader.loadResource(
      visual_frame_ptr, kResourceIdA,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType));
  EXPECT_TRUE(loader.loadResource(
      visual_frame_ptr, kResourceIdB,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType));
}
}  // namespace map_api

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
