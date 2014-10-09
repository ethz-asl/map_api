#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "./visual_frame_resource_loader_fixture.h"

namespace map_api {

TEST_F(ResourceLoaderTest, ShouldFindResourceIds) {
  ResourceLoader loader = ResourceLoader(kTableName);
  std::unordered_set<std::string> resource_ids, resource_ids_2;

  // Get all ids for the resources of type RawImage for visual frame 0xA
  loader.getResourceIdsOfType(
      ResourceLoaderTest::kVisualFrameId1,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      &resource_ids);

  EXPECT_EQ(resource_ids.size(), 2);
  EXPECT_NE(resource_ids.find(kResourceIdA), resource_ids.end());
  EXPECT_NE(resource_ids.find(kResourceIdB), resource_ids.end());

  // Get all ids for the resources of type DisparityImage for visual frame 0xA
  loader.getResourceIdsOfType(
      ResourceLoaderTest::kVisualFrameId1,
      common::ResourceLoaderBase::kVisualFrameResourceDisparityImageType,
      &resource_ids_2);

  EXPECT_EQ(resource_ids_2.size(), 20);
  for (auto id : ResourceLoaderTest::kDisparityMapIds1) {
    EXPECT_NE(resource_ids_2.find(id), resource_ids.end());
  }
}

TEST_F(ResourceLoaderTest, ShouldLoadAndStoreResources) {
  ResourceLoader loader = ResourceLoader(kTableName);
  VisualFrameDummy* dummy_visual_frame_ptr = new VisualFrameDummy();

  // Load two resources of type RawImage for visual frame 0xA
  EXPECT_TRUE(loader.loadResource(
      kResourceIdA,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      dummy_visual_frame_ptr));
  EXPECT_TRUE(loader.loadResource(
      kResourceIdB,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      dummy_visual_frame_ptr));

  // Check if resource loader stored the loaded resource in the visual frame
  EXPECT_EQ(dummy_visual_frame_ptr->resourcesStored_.size(), 2);
  EXPECT_NE(dummy_visual_frame_ptr->resourcesStored_.find(kResourceIdA),
            dummy_visual_frame_ptr->resourcesStored_.end());
  EXPECT_NE(dummy_visual_frame_ptr->resourcesStored_.find(kResourceIdB),
            dummy_visual_frame_ptr->resourcesStored_.end());

  delete dummy_visual_frame_ptr;
}

TEST_F(ResourceLoaderTest, ShouldReleaseResourcesCorrectly) {
  ResourceLoader loader = ResourceLoader(kTableName);
  VisualFrameDummy* dummy_visual_frame_ptr_1 =
      new VisualFrameDummy();  // ID=0xA
  VisualFrameDummy* dummy_visual_frame_ptr_2 =
      new VisualFrameDummy();  // ID=0xB
  VisualFrameDummy* dummy_visual_frame_ptr_3 =
      new VisualFrameDummy();  // ID=0xC

  // Load two resources of type RawImage for visual frame 1
  EXPECT_TRUE(loader.loadResource(
      kResourceIdA,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      dummy_visual_frame_ptr_1));
  EXPECT_TRUE(loader.loadResource(
      kResourceIdB,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      dummy_visual_frame_ptr_1));

  // Check if resource loader stored the loaded resource in the visual frame
  EXPECT_EQ(dummy_visual_frame_ptr_1->resourcesStored_.size(), 2);
  EXPECT_NE(dummy_visual_frame_ptr_1->resourcesStored_.find(kResourceIdA),
            dummy_visual_frame_ptr_1->resourcesStored_.end());
  EXPECT_NE(dummy_visual_frame_ptr_1->resourcesStored_.find(kResourceIdB),
            dummy_visual_frame_ptr_1->resourcesStored_.end());

  std::cout << "load 20 disparity maps for visual frame 0xA" << std::endl;
  // Load 20 resources of type DisparityMap for visual frame 1
  for (auto id : ResourceLoaderTest::kDisparityMapIds1) {
    EXPECT_TRUE(loader.loadResource(
        id, common::ResourceLoaderBase::kVisualFrameResourceDisparityImageType,
        dummy_visual_frame_ptr_1));
    EXPECT_NE(dummy_visual_frame_ptr_1->resourcesStored_.find(id),
              dummy_visual_frame_ptr_1->resourcesStored_.end());
  }
  EXPECT_EQ(dummy_visual_frame_ptr_1->resourcesStored_.size(), 22);

  std::cout << "load 10 disparity maps for visual frame 0xB" << std::endl;
  // Load 10 resources of type DisparityMap for visual frame 2
  for (auto id : ResourceLoaderTest::kDisparityMapIds2) {
    EXPECT_TRUE(loader.loadResource(
        id, common::ResourceLoaderBase::kVisualFrameResourceDisparityImageType,
        dummy_visual_frame_ptr_2));
    EXPECT_NE(dummy_visual_frame_ptr_2->resourcesStored_.find(id),
              dummy_visual_frame_ptr_2->resourcesStored_.end());
  }

  // Check if loader released 10 resources to accommodate the 10 new ones
  EXPECT_EQ(dummy_visual_frame_ptr_2->resourcesStored_.size(), 10);
  EXPECT_EQ(dummy_visual_frame_ptr_1->resourcesStored_.size(), 12);

  std::cout << "load 15 disparity maps for visual frame 0xC" << std::endl;
  // Load 15 resources of type DisparityMap for visual frame 3
  for (auto id : ResourceLoaderTest::kDisparityMapIds3) {
    EXPECT_TRUE(loader.loadResource(
        id, common::ResourceLoaderBase::kVisualFrameResourceDisparityImageType,
        dummy_visual_frame_ptr_3));
    EXPECT_NE(dummy_visual_frame_ptr_3->resourcesStored_.find(id),
              dummy_visual_frame_ptr_3->resourcesStored_.end());
  }

  // Check if loader released 10 resources for visual frame 1 and 5 resources
  // for visual frame 2 to accommodate the 15 new ones
  EXPECT_EQ(dummy_visual_frame_ptr_3->resourcesStored_.size(), 15);
  EXPECT_EQ(dummy_visual_frame_ptr_2->resourcesStored_.size(), 5);
  EXPECT_EQ(dummy_visual_frame_ptr_1->resourcesStored_.size(), 2);

  delete dummy_visual_frame_ptr_1;
  delete dummy_visual_frame_ptr_2;
  delete dummy_visual_frame_ptr_3;
}

}  // namespace map_api

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
