#include <glog/logging.h>
#include <gtest/gtest.h>

#include "dmap/core.h"
#include "dmap/test/testing-entrypoint.h"

namespace dmap {

TEST(CoreTest, validInit) {
  Core* instance = Core::instance();
  EXPECT_EQ(nullptr, instance);
  Core::initializeInstance();
  instance = Core::instance();
  EXPECT_NE(nullptr, instance);
  EXPECT_TRUE(instance->isInitialized());
  instance->kill();
  EXPECT_EQ(nullptr, Core::instance());
}

}  // namespace dmap

MAP_API_UNITTEST_ENTRYPOINT
