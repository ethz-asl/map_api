#include <set>
#include <sys/types.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "map-api/hub.h"
#include "map-api/ipc.h"
#include "map-api/peer-id.h"
#include "map-api/raft.h"
#include "map-api/test/testing-entrypoint.h"
#include "./consensus_fixture.h"

namespace map_api {

TEST_F(ConsensusFixture, DISABLED_LeaderElection) {
  const uint64_t kProcesses = 5;
  enum Barriers {
    INIT,
    PEERS_SETUP,
    DIE
  };

  RaftCluster::instance().registerHandlers();
  pid_t pid = getpid();
  LOG(INFO) << "Peer Id " << RaftCluster::instance().self_id() << " : PID "
            << pid;

  // Main parent process
  if (getSubprocessId() == 0) {
    std::ostringstream extra_flags_ss;
    for (uint64_t i = 1u; i < kProcesses; ++i) {
      launchSubprocess(i);
    }

    IPC::barrier(INIT, kProcesses - 1);

    // Find peers in the network, add them to raft cluster
    std::set<PeerId> peer_list;
    Hub::instance().getPeers(&peer_list);
    for (const PeerId& peer : peer_list) {
      RaftCluster::instance().addPeerBeforeStart(peer);
    }

    // IPC Push info
    IPC::barrier(PEERS_SETUP, kProcesses - 1);
    RaftCluster::instance().start();
    while (true) {
      // Do nothing
    }
  } else {  // Subprocesses
    IPC::barrier(INIT, kProcesses - 1);
    // Find peers in the network, add them to raft cluster
    std::set<PeerId> peer_list;
    Hub::instance().getPeers(&peer_list);
    for (const PeerId& peer : peer_list) {
      RaftCluster::instance().addPeerBeforeStart(peer);
    }
    IPC::barrier(PEERS_SETUP, kProcesses - 1);
    // IPC::barrier(DIE, kProcesses - 1);
    RaftCluster::instance().start();
    while (true) {
      // Do nothing
    }
  }
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
