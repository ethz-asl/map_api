#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "./visual_frame_resource_loader_fixture.h"

namespace map_api {

TEST_F(ResourceLoaderFixture, ShouldFindResourceIds) {
  ResourceLoader loader = ResourceLoader(kTableName);
  std::unordered_set<std::string> resource_ids, resource_ids_2;

  // Get all ids for the resources of type RawImage for visual frame 0xA
  loader.getResourceIdsOfType(
      kVisualFrameId1,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      &resource_ids);

  EXPECT_EQ(2u, resource_ids.size());
  EXPECT_NE(resource_ids.end(), resource_ids.find(kResourceIdA));
  EXPECT_NE(resource_ids.end(), resource_ids.find(kResourceIdB));

  // Get all ids for the resources of type DepthImage for visual frame 0xA
  loader.getResourceIdsOfType(
      kVisualFrameId1,
      common::ResourceLoaderBase::kVisualFrameResourceDepthMapType,
      &resource_ids_2);

  EXPECT_EQ(20u, resource_ids_2.size());
  for (auto id : kDepthMapIds1) {
    EXPECT_NE(resource_ids.end(), resource_ids_2.find(id));
  }
}

TEST_F(ResourceLoaderFixture, ShouldLoadAndStoreResources) {
  ResourceLoader loader = ResourceLoader(kTableName);
  VisualFrameDummy dummy_visual_frame;

  // Load two resources of type RawImage for visual frame 0xA
  EXPECT_TRUE(loader.loadResource(
      kResourceIdA,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      &dummy_visual_frame));
  EXPECT_TRUE(loader.loadResource(
      kResourceIdB,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      &dummy_visual_frame));

  // Check if resource loader stored the loaded resource in the visual frame
  EXPECT_EQ(2u, dummy_visual_frame.resourcesStored_.size());
  EXPECT_NE(dummy_visual_frame.resourcesStored_.end(),
            dummy_visual_frame.resourcesStored_.find(kResourceIdA));
  EXPECT_NE(dummy_visual_frame.resourcesStored_.end(),
            dummy_visual_frame.resourcesStored_.find(kResourceIdB));
}

TEST_F(ResourceLoaderFixture, ShouldReleaseResourcesCorrectly) {
  ResourceLoader loader = ResourceLoader(kTableName);
  VisualFrameDummy dummy_visual_frame_1;  // ID=0xA
  VisualFrameDummy dummy_visual_frame_2;  // ID=0xB
  VisualFrameDummy dummy_visual_frame_3;  // ID=0xC

  // Load two resources of type RawImage for visual frame 1
  EXPECT_TRUE(loader.loadResource(
      kResourceIdA,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      &dummy_visual_frame_1));
  EXPECT_TRUE(loader.loadResource(
      kResourceIdB,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      &dummy_visual_frame_1));

  // Check if resource loader stored the loaded resource in the visual frame
  EXPECT_EQ(2u, dummy_visual_frame_1.resourcesStored_.size());
  EXPECT_NE(dummy_visual_frame_1.resourcesStored_.end(),
            dummy_visual_frame_1.resourcesStored_.find(kResourceIdA));
  EXPECT_NE(dummy_visual_frame_1.resourcesStored_.end(),
            dummy_visual_frame_1.resourcesStored_.find(kResourceIdB));

  // Load 20 resources of type DepthMap for visual frame 1
  for (auto id : kDepthMapIds1) {
    EXPECT_TRUE(loader.loadResource(
        id, common::ResourceLoaderBase::kVisualFrameResourceDepthMapType,
        &dummy_visual_frame_1));
    EXPECT_NE(dummy_visual_frame_1.resourcesStored_.find(id),
              dummy_visual_frame_1.resourcesStored_.end());
  }
  EXPECT_EQ(22u, dummy_visual_frame_1.resourcesStored_.size());

  // Load 10 resources of type DepthMap for visual frame 2
  for (auto id : kDepthMapIds2) {
    EXPECT_TRUE(loader.loadResource(
        id, common::ResourceLoaderBase::kVisualFrameResourceDepthMapType,
        &dummy_visual_frame_2));
    EXPECT_NE(dummy_visual_frame_2.resourcesStored_.end(),
              dummy_visual_frame_2.resourcesStored_.find(id));
  }

  // Check if loader released 10 resources to accommodate the 10 new ones
  EXPECT_EQ(10u, dummy_visual_frame_2.resourcesStored_.size());
  EXPECT_EQ(12u, dummy_visual_frame_1.resourcesStored_.size());

  // Load 15 resources of type DepthMap for visual frame 3
  for (auto id : kDepthMapIds3) {
    EXPECT_TRUE(loader.loadResource(
        id, common::ResourceLoaderBase::kVisualFrameResourceDepthMapType,
        &dummy_visual_frame_3));
    EXPECT_NE(dummy_visual_frame_3.resourcesStored_.end(),
              dummy_visual_frame_3.resourcesStored_.find(id));
  }

  // Check if loader released 10 resources for visual frame 1 and 5 resources
  // for visual frame 2 to accommodate the 15 new ones
  EXPECT_EQ(15u, dummy_visual_frame_3.resourcesStored_.size());
  EXPECT_EQ(5u, dummy_visual_frame_2.resourcesStored_.size());
  EXPECT_EQ(2u, dummy_visual_frame_1.resourcesStored_.size());
}

}  // namespace map_api

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
