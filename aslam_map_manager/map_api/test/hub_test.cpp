#include <glog/logging.h>
#include <gtest/gtest.h>

#include <map-api/map-api-core.h>

#include "multiprocess_fixture.cpp"

using namespace map_api;

class MultiprocessTest;

TEST_F(MultiprocessTest, LaunchTest) {
  if (getSubprocessId() == 0) {
    uint64_t id = launchSubProcess();
  }
}
