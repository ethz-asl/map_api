#include <glog/logging.h>
#include <gtest/gtest.h>

#include <map-api/ipc.h>
#include <map-api/map-api-core.h>

#include "multiprocess_fixture.cpp"

using namespace map_api;

class MultiprocessTest;

TEST_F(MultiprocessTest, LaunchTest) {
  enum Barriers {BEFORE_COUNT, AFTER_COUNT};
  IPC::init();
  MapApiCore::getInstance();
  if (getSubprocessId() == 0) {
    EXPECT_EQ(0, MapApiHub::getInstance().peerSize());
    uint64_t id = launchSubprocess();
    IPC::barrier(BEFORE_COUNT, 1);
    EXPECT_EQ(1, MapApiHub::getInstance().peerSize());
    IPC::barrier(AFTER_COUNT, 1);
    collectSubprocess(id);
  } else {
    IPC::barrier(BEFORE_COUNT, 1);
    IPC::barrier(AFTER_COUNT, 1);
  }
}
