#include <glog/logging.h>
#include <gtest/gtest.h>

#include <map-api/map-api-core.h>

#include "multiprocess_fixture.cpp"

using namespace map_api;

class MultiprocessTest;

TEST_F(MultiprocessTest, LaunchTest) {
  MapApiCore::getInstance();
  // somehow this is necessary for the flag "ipPort" to be seen - a linker thing
  if (getSubprocessId() == 0) {
    EXPECT_EQ(0, MapApiHub::getInstance().peerSize());
    uint64_t id = launchSubProcess();
    sleep(1); // using cheap sleep tricks until I realize a IPC barrier
    EXPECT_EQ(1, MapApiHub::getInstance().peerSize());
  } else {
    sleep(2);
  }
}
