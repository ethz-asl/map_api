#ifndef MAP_API_VISUAL_FRAME_RESOURCE_LOADER_FIXTURE_H_
#define MAP_API_VISUAL_FRAME_RESOURCE_LOADER_FIXTURE_H_

#include <set>
#include <string>

#include <multiagent_mapping_common/test/testing_predicates.h>

#include "map-api/visual-frame-resource-loader.h"

#include "./map_api_fixture.h"

namespace map_api {

// Replaces the VisualFrame class and simulates resource storing and releasing
class VisualFrameDummy : public common::VisualFrameBase {
 public:
  bool releaseResource(const std::string& resource_id_hex_string) {
    EXPECT_EQ(resourcesStored_.erase(resource_id_hex_string), 1);
    return true;
  }
  bool storeResource(const std::string& resource_id_hex_string,
                     const cv::Mat& resource) {
    EXPECT_TRUE(resource.data && !resource.empty());
    resourcesStored_.insert(resource_id_hex_string);
    return true;
  }
  std::unordered_set<std::string> resourcesStored_;
};

// Set up DB and DB entries for tests
class ResourceLoaderTest : public MapApiFixture {
 public:
  virtual ~ResourceLoaderTest() {}

  enum VisualFrameTableFields {
    kVisualFrameTableUri,
    kVisualFrameTableType,
    kVisualFrameTableVisualFrameId
  };

  virtual void SetUp() override {
    MultiprocessTest::SetUp();

    // Set up DB table
    std::unique_ptr<TableDescriptor> descriptor(new TableDescriptor);
    descriptor->setName(kTableName);
    descriptor->addField<std::string>(kVisualFrameTableUri);
    descriptor->addField<int>(kVisualFrameTableType);
    descriptor->addField<Id>(kVisualFrameTableVisualFrameId);
    NetTableManager::instance().addTable(CRTable::Type::CRU, &descriptor);
    table_ = &NetTableManager::instance().getTable(kTableName);

    // Set up chunk
    Id chunk_id;
    chunk_id.fromHexString(kChunkId);
    Chunk* chunk = table_->newChunk(chunk_id);

    // Set up two visual frame ids (ID=0xA, ID=0xB and ID=0xC)
    Id visual_frame_id_1, visual_frame_id_2, visual_frame_id_3;
    visual_frame_id_1.fromHexString(kVisualFrameId1);
    visual_frame_id_2.fromHexString(kVisualFrameId2);
    visual_frame_id_3.fromHexString(kVisualFrameId3);

    // Generate two DB entries of type RawImage for visual frame 0xA
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
    to_insert_1->set<Id>(kVisualFrameTableVisualFrameId, visual_frame_id_1);
    transaction_1.insert(table_, chunk, to_insert_1);
    EXPECT_TRUE(transaction_1.commit());

    std::shared_ptr<Revision> to_insert_2 = table_->getTemplate();
    Transaction transaction_2;
    Id insert_id_2;
    insert_id_2.fromHexString(kResourceIdB);
    to_insert_2->setId(insert_id_2);
    to_insert_2->set<std::string>(kVisualFrameTableUri, "test-data/no.png");
    to_insert_2->set<int>(
        kVisualFrameTableType,
        common::ResourceLoaderBase::kVisualFrameResourceRawImageType);
    to_insert_2->set<Id>(kVisualFrameTableVisualFrameId, visual_frame_id_1);
    transaction_2.insert(table_, chunk, to_insert_2);
    EXPECT_TRUE(transaction_2.commit());

    // Create DB entries of type DisparityMap for visual frame 0xA
    for (auto id : kDisparityMapIds1) {
      std::shared_ptr<Revision> to_insert_i = table_->getTemplate();
      Transaction transaction_i;
      Id insert_id_i;
      insert_id_i.fromHexString(id);
      to_insert_i->setId(insert_id_i);
      to_insert_i->set<std::string>(kVisualFrameTableUri,
                                    "test-data/problem.jpg");
      to_insert_i->set<int>(
          kVisualFrameTableType,
          common::ResourceLoaderBase::kVisualFrameResourceDisparityImageType);
      to_insert_i->set<Id>(kVisualFrameTableVisualFrameId, visual_frame_id_1);
      transaction_i.insert(table_, chunk, to_insert_i);
      EXPECT_TRUE(transaction_i.commit());
    }

    // Create DB entries of type DisparityMap for visual frame 0xB
    for (auto id : kDisparityMapIds2) {
      std::shared_ptr<Revision> to_insert_i = table_->getTemplate();
      Transaction transaction_i;
      Id insert_id_i;
      insert_id_i.fromHexString(id);
      to_insert_i->setId(insert_id_i);
      to_insert_i->set<std::string>(kVisualFrameTableUri, "test-data/no.png");
      to_insert_i->set<int>(
          kVisualFrameTableType,
          common::ResourceLoaderBase::kVisualFrameResourceDisparityImageType);
      to_insert_i->set<Id>(kVisualFrameTableVisualFrameId, visual_frame_id_2);
      transaction_i.insert(table_, chunk, to_insert_i);
      EXPECT_TRUE(transaction_i.commit());
    }

    // Create DB entries of type DisparityMap for visual frame 0xC
    for (auto id : kDisparityMapIds3) {
      std::shared_ptr<Revision> to_insert_i = table_->getTemplate();
      Transaction transaction_i;
      Id insert_id_i;
      insert_id_i.fromHexString(id);
      to_insert_i->setId(insert_id_i);
      to_insert_i->set<std::string>(kVisualFrameTableUri, "test-data/no.png");
      to_insert_i->set<int>(
          kVisualFrameTableType,
          common::ResourceLoaderBase::kVisualFrameResourceDisparityImageType);
      to_insert_i->set<Id>(kVisualFrameTableVisualFrameId, visual_frame_id_3);
      transaction_i.insert(table_, chunk, to_insert_i);
      EXPECT_TRUE(transaction_i.commit());
    }
  }

  virtual void TearDown() override { MultiprocessTest::TearDown(); }

  static const std::string kTableName;
  static const std::string kChunkId;
  static const std::string kVisualFrameId1;
  static const std::string kVisualFrameId2;
  static const std::string kVisualFrameId3;
  static const std::string kResourceIdA;
  static const std::string kResourceIdB;
  static std::unordered_set<std::string> kDisparityMapIds1;
  static std::unordered_set<std::string> kDisparityMapIds2;
  static std::unordered_set<std::string> kDisparityMapIds3;

 private:
  NetTable* table_;
};

std::unordered_set<std::string> initIdSet(int start, int number) {
  std::unordered_set<std::string> tmp_disparity_map_ids;
  for (int i = start; i <= (start + number - 1); ++i) {
    if (i < 10) {
      tmp_disparity_map_ids.insert("0000000000000000000000000000000" +
                                   std::to_string(i));
    } else {
      tmp_disparity_map_ids.insert("000000000000000000000000000000" +
                                   std::to_string(i));
    }
  }
  return tmp_disparity_map_ids;
}

const std::string ResourceLoaderTest::kTableName =
    "visual_frame_resource_test_table";

const std::string ResourceLoaderTest::kChunkId =
    "0000000000000000000000000000000F";
const std::string ResourceLoaderTest::kVisualFrameId1 =
    "0000000000000000000000000000000A";
const std::string ResourceLoaderTest::kVisualFrameId2 =
    "0000000000000000000000000000000B";
const std::string ResourceLoaderTest::kVisualFrameId3 =
    "0000000000000000000000000000000C";

const std::string ResourceLoaderTest::kResourceIdA =
    "00000000000000000000000000000777";
const std::string ResourceLoaderTest::kResourceIdB =
    "00000000000000000000000000000888";
std::unordered_set<std::string> ResourceLoaderTest::kDisparityMapIds1(
    initIdSet(1, 20));
std::unordered_set<std::string> ResourceLoaderTest::kDisparityMapIds2(
    initIdSet(31, 10));
std::unordered_set<std::string> ResourceLoaderTest::kDisparityMapIds3(
    initIdSet(51, 15));

}  // namespace map_api

#endif  // MAP_API_VISUAL_FRAME_RESOURCE_LOADER_FIXTURE_H_
