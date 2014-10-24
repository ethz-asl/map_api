#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent-mapping-common/test/testing-entrypoint.h>

#include <map-api/core.h>
#include <map-api/ipc.h>
#include <map-api/test/map_api_fixture.h>

namespace map_api {

TEST_F(MapApiFixture, LaunchTest) {
  enum Processes {
    ROOT,
    SLAVE
  };
  enum Barriers {
    BEFORE_COUNT,
    AFTER_COUNT
  };
  if (getSubprocessId() == ROOT) {
    EXPECT_EQ(0, Hub::instance().peerSize());
    launchSubprocess(SLAVE);
    IPC::barrier(BEFORE_COUNT, 1);
    EXPECT_EQ(1, Hub::instance().peerSize());
    IPC::barrier(AFTER_COUNT, 1);
  } else {
    IPC::barrier(BEFORE_COUNT, 1);
    IPC::barrier(AFTER_COUNT, 1);
  }
}

}  // namespace map_api

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
