#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "map-api/map-api-core.h"

using namespace map_api;

TEST(MapApiCore, validInit) {
  MapApiCore &instance = MapApiCore::instance();
  EXPECT_TRUE(instance.isInitialized());
  instance.kill();
  EXPECT_FALSE(instance.isInitialized());
}

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
