#include <multiagent-mapping-common/test/testing-entrypoint.h>

#include "./visual_frame_resource_loader_fixture.h"

namespace map_api {

TEST_F(ResourceLoaderFixture, ShouldFindResourceIds) {
  EXPECT_EQ(20u, kDepthMapIds1.size());

  std::unique_ptr<common::ResourceLoaderBase> loader(
      new ResourceLoader(kTableName));
  std::unordered_set<std::string> resource_ids, resource_ids_2;

  // Get all ids for the resources of type RawImage for visual frame 0xA
  loader->getResourceIdsOfType(
      kVisualFrameId1,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      &resource_ids);

  EXPECT_EQ(2u, resource_ids.size());
  EXPECT_NE(resource_ids.end(), resource_ids.find(kResourceIdA));
  EXPECT_NE(resource_ids.end(), resource_ids.find(kResourceIdB));

  // Get all ids for the resources of type DepthImage for visual frame 0xA
  loader->getResourceIdsOfType(
      kVisualFrameId1,
      common::ResourceLoaderBase::kVisualFrameResourceDepthMapType,
      &resource_ids_2);

  EXPECT_EQ(20u, resource_ids_2.size());
  for (const std::string& id : kDepthMapIds1) {
    EXPECT_NE(resource_ids.end(), resource_ids_2.find(id));
  }
}

TEST_F(ResourceLoaderFixture, ShouldLoadAndStoreResources) {
  std::unique_ptr<common::ResourceLoaderBase> loader(
      new ResourceLoader(kTableName));
  aslam::VisualFrame visual_frame;
  aslam::FrameId visual_frame_id;
  visual_frame_id.fromHexString(kVisualFrameId1);
  visual_frame.setId(visual_frame_id);

  // Check if visual frame channels are empty
  EXPECT_FALSE(visual_frame.hasChannel(kResourceIdA));
  EXPECT_FALSE(visual_frame.hasChannel(kResourceIdB));

  // Load two resources of type RawImage for visual frame 0xA
  EXPECT_TRUE(loader->loadResource(
      kResourceIdA,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      &visual_frame));
  EXPECT_TRUE(loader->loadResource(
      kResourceIdB,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      &visual_frame));

  // Check if resource loader stored the loaded resource in the visual frame
  EXPECT_TRUE(visual_frame.hasChannel(kResourceIdA));
  EXPECT_TRUE(visual_frame.hasChannel(kResourceIdB));
  EXPECT_FALSE(visual_frame.getChannelData<cv::Mat>(kResourceIdA).empty());
  EXPECT_FALSE(visual_frame.getChannelData<cv::Mat>(kResourceIdA).empty());
}

TEST_F(ResourceLoaderFixture, ShouldReleaseResourcesCorrectly) {
  EXPECT_EQ(20u, kDepthMapIds1.size());
  EXPECT_EQ(10u, kDepthMapIds2.size());
  EXPECT_EQ(15u, kDepthMapIds3.size());

  std::unique_ptr<common::ResourceLoaderBase> loader(
      new ResourceLoader(kTableName));
  aslam::VisualFrame visual_frame_1, visual_frame_2, visual_frame_3;
  aslam::FrameId visual_frame_id_1, visual_frame_id_2, visual_frame_id_3;
  visual_frame_id_1.fromHexString(kVisualFrameId1);
  visual_frame_id_2.fromHexString(kVisualFrameId2);
  visual_frame_id_3.fromHexString(kVisualFrameId3);
  visual_frame_1.setId(visual_frame_id_1);  // ID=0xA
  visual_frame_2.setId(visual_frame_id_2);  // ID=0xB
  visual_frame_3.setId(visual_frame_id_3);  // ID=0xC

  // Load two resources of type RawImage for visual frame 1
  EXPECT_TRUE(loader->loadResource(
      kResourceIdA,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      &visual_frame_1));
  EXPECT_TRUE(loader->loadResource(
      kResourceIdB,
      common::ResourceLoaderBase::kVisualFrameResourceRawImageType,
      &visual_frame_1));

  // Check if resource loader stored the loaded resource in the visual frame
  EXPECT_TRUE(visual_frame_1.hasChannel(kResourceIdA));
  EXPECT_TRUE(visual_frame_1.hasChannel(kResourceIdB));
  EXPECT_FALSE(visual_frame_1.getChannelData<cv::Mat>(kResourceIdA).empty());
  EXPECT_FALSE(visual_frame_1.getChannelData<cv::Mat>(kResourceIdA).empty());

  // Load 20 resources of type DepthMap for visual frame 1
  for (const std::string& id : kDepthMapIds1) {
    EXPECT_TRUE(loader->loadResource(
        id, common::ResourceLoaderBase::kVisualFrameResourceDepthMapType,
        &visual_frame_1));
    EXPECT_TRUE(visual_frame_1.hasChannel(id));
    EXPECT_FALSE(visual_frame_1.getChannelData<cv::Mat>(id).empty());
  }

  // Check if still all 22 resources are loaded, e.g. no resources have been
  // released
  for (const std::string& id : kDepthMapIds1) {
    EXPECT_TRUE(visual_frame_1.hasChannel(id));
    EXPECT_FALSE(visual_frame_1.getChannelData<cv::Mat>(id).empty());
  }
  EXPECT_TRUE(visual_frame_1.hasChannel(kResourceIdA));
  EXPECT_TRUE(visual_frame_1.hasChannel(kResourceIdB));
  EXPECT_FALSE(visual_frame_1.getChannelData<cv::Mat>(kResourceIdA).empty());
  EXPECT_FALSE(visual_frame_1.getChannelData<cv::Mat>(kResourceIdA).empty());

  // Load 10 resources of type DepthMap for visual frame 2
  for (const std::string& id : kDepthMapIds2) {
    EXPECT_TRUE(loader->loadResource(
        id, common::ResourceLoaderBase::kVisualFrameResourceDepthMapType,
        &visual_frame_2));
    EXPECT_TRUE(visual_frame_2.hasChannel(id));
    EXPECT_FALSE(visual_frame_2.getChannelData<cv::Mat>(id).empty());
  }

  // All 10 loaded resources of type DepthMap for visual frame 2 should be
  // loaded
  for (const std::string& id : kDepthMapIds2) {
    EXPECT_TRUE(visual_frame_2.hasChannel(id));
    EXPECT_FALSE(visual_frame_2.getChannelData<cv::Mat>(id).empty());
  }

  // Loader should have released 10 resources of type DepthMap of
  // visual frame 1 to accommodate the 10 new ones of visual frame 2
  int release_counter_1 = 0, loaded_counter_1 = 0;
  for (const std::string& id : kDepthMapIds1) {
    if (visual_frame_1.hasChannel(id)) {
      if (!visual_frame_1.getChannelData<cv::Mat>(id).empty()) {
        ++loaded_counter_1;
      } else {
        ++release_counter_1;
      }
    } else {
      FAIL() << "Released resources should push an empty cv::Mat onto the"
             << "channel not delete it";
    }
  }
  EXPECT_EQ(10, release_counter_1);
  EXPECT_EQ(10, loaded_counter_1);

  // The resources of type raw image should still be loaded
  EXPECT_TRUE(visual_frame_1.hasChannel(kResourceIdA));
  EXPECT_TRUE(visual_frame_1.hasChannel(kResourceIdB));
  EXPECT_FALSE(visual_frame_1.getChannelData<cv::Mat>(kResourceIdA).empty());
  EXPECT_FALSE(visual_frame_1.getChannelData<cv::Mat>(kResourceIdA).empty());

  // Load 15 resources of type DepthMap for visual frame 3
  for (const std::string& id : kDepthMapIds3) {
    EXPECT_TRUE(loader->loadResource(
        id, common::ResourceLoaderBase::kVisualFrameResourceDepthMapType,
        &visual_frame_3));
    EXPECT_TRUE(visual_frame_3.hasChannel(id));
    EXPECT_FALSE(visual_frame_3.getChannelData<cv::Mat>(id).empty());
  }

  // All 15 loaded resources of type DepthMap for visual frame 3 should be
  // loaded
  for (const std::string& id : kDepthMapIds3) {
    EXPECT_TRUE(visual_frame_3.hasChannel(id));
    EXPECT_FALSE(visual_frame_3.getChannelData<cv::Mat>(id).empty());
  }

  // Loader should have released all resources of type DepthMap of
  // visual frame 1 to accommodate the 15 new ones of visual frame 3
  for (const std::string& id : kDepthMapIds1) {
    if (visual_frame_1.hasChannel(id)) {
      EXPECT_TRUE(visual_frame_1.getChannelData<cv::Mat>(id).empty());
    } else {
      FAIL() << "Released resources should push an empty cv::Mat onto the"
             << "channel not delete it";
    }
  }

  // Loader should have released 5 resources of type DepthMap of
  // visual frame 2 to accommodate the 15 new ones of visual frame 3
  int release_counter_2 = 0, loaded_counter_2 = 0;
  for (const std::string& id : kDepthMapIds2) {
    if (visual_frame_2.hasChannel(id)) {
      if (!visual_frame_2.getChannelData<cv::Mat>(id).empty()) {
        ++loaded_counter_2;
      } else {
        ++release_counter_2;
      }
    } else {
      FAIL() << "Released resources should push an empty cv::Mat onto the"
             << "channel not delete it";
    }
  }
  EXPECT_EQ(5, release_counter_2);
  EXPECT_EQ(5, loaded_counter_2);

  // The resources of type raw image should still be loaded
  EXPECT_TRUE(visual_frame_1.hasChannel(kResourceIdA));
  EXPECT_TRUE(visual_frame_1.hasChannel(kResourceIdB));
  EXPECT_FALSE(visual_frame_1.getChannelData<cv::Mat>(kResourceIdA).empty());
  EXPECT_FALSE(visual_frame_1.getChannelData<cv::Mat>(kResourceIdA).empty());
}

}  // namespace map_api

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
