#include <glog/logging.h>
#include <gtest/gtest.h>

#include "map-api/core.h"
#include "map-api/test/testing-entrypoint.h"

namespace map_api {

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

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
