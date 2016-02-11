#include <glog/logging.h>
#include <gtest/gtest.h>

#include "dmap/core.h"
#include "dmap/hub.h"
#include "dmap/ipc.h"
#include "dmap/test/testing-entrypoint.h"
#include "./dmap_fixture.h"

namespace dmap {

class HubTest : public MapApiFixture {};

TEST_F(HubTest, LaunchTest) {
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

}  // namespace dmap

DMAP_UNITTEST_ENTRYPOINT
