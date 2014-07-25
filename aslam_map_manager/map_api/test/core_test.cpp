#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "map-api/map-api-core.h"

using namespace map_api;

TEST(MapApiCore, validInit) {
  MapApiCore* instance = MapApiCore::instance();
  EXPECT_EQ(nullptr, instance);
  MapApiCore::initializeInstance();
  instance = MapApiCore::instance();
  EXPECT_NE(nullptr, instance);
  EXPECT_TRUE(instance->isInitialized());
  instance->kill();
  EXPECT_EQ(nullptr, MapApiCore::instance());
}

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
