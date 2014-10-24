#include <map-api/core.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent-mapping-common/test/testing-entrypoint.h>


using namespace map_api;

TEST(Core, validInit) {
  Core* instance = Core::instance();
  EXPECT_EQ(nullptr, instance);
  Core::initializeInstance();
  instance = Core::instance();
  EXPECT_NE(nullptr, instance);
  EXPECT_TRUE(instance->isInitialized());
  instance->kill();
  EXPECT_EQ(nullptr, Core::instance());
}

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
