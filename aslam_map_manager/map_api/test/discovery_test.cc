#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "map-api/hub.h"
#include "map-api/discovery.h"
#include "map-api/file-discovery.h"
#include "map-api/server-discovery.h"
#include "./map_api_fixture.h"

DECLARE_string(discovery_mode);

namespace map_api {

class DiscoveryTest : public MapApiFixture,
                      public ::testing::WithParamInterface<const char*> {
 protected:
  virtual void SetUp() {
    FLAGS_discovery_mode = GetParam();
    if (strcmp(GetParam(), "server") == 0) {
      launchDiscoveryServer();
    }
    MapApiFixture::SetUp();
  }

  virtual void TearDown() {
    MapApiFixture::TearDown();
    if (strcmp(GetParam(), "server") == 0) {
      killDiscoveryServer();
    }
  }

  void launchDiscoveryServer() {
    std::ostringstream command;
    std::string this_executable = getSelfpath();
    unsigned last_slash = this_executable.find_last_of("/");
    command << this_executable.substr(0, last_slash + 1) << "discovery-server";
    discovery_server_ = popen2(command.str().c_str());
  }

  void killDiscoveryServer() { kill(discovery_server_, SIGINT); }

  // http://stackoverflow.com/questions/548063/kill-a-process-started-with-popen
  pid_t popen2(const char* command) {
    pid_t pid = fork();
    CHECK_GE(pid, 0);
    if (pid == 0) {
      execl(command, command, NULL);
      perror("execl");
      exit(1);
    }
    return pid;
  }

  static void getPeersGrindThread() {
    std::set<PeerId> peers;
    for (size_t i = 0; i < kGetPeersGrindIterations; ++i) {
      Hub::instance().getPeers(&peers);
    }
  }

  static constexpr size_t kGetPeersGrindIterations = 1000;
  pid_t discovery_server_;
};

TEST_P(DiscoveryTest, ThreadSafety) {
  std::thread a(getPeersGrindThread), b(getPeersGrindThread);
  a.join();
  b.join();
}

INSTANTIATE_TEST_CASE_P(DiscoveryInstances, DiscoveryTest,
                        ::testing::Values("file", "server"));

}  // namespace map_api

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
