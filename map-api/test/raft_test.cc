#include <set>
#include <sys/types.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "map-api/hub.h"
#include "map-api/ipc.h"
#include "map-api/peer-id.h"
#include "map-api/raft-node.h"
#include "map-api/test/testing-entrypoint.h"
#include "./consensus_fixture.h"

namespace map_api {

TEST_F(ConsensusFixture, LeaderElection) {
  const uint64_t kProcesses = 5;
  enum Barriers {
    INIT,
    PEERS_SETUP,
    DIE
  };

  pid_t pid = getpid();
  VLOG(1) << "Peer Id " << RaftNode::instance().self_id() << " : PID " << pid;

  if (getSubprocessId() == 0) {
    for (uint64_t i = 1u; i < kProcesses; ++i) {
      launchSubprocess(i);
    }
  }
  IPC::barrier(INIT, kProcesses - 1);
  // Find peers in the network and add them to raft cluster.
  std::set<PeerId> peer_list;
  Hub::instance().getPeers(&peer_list);
  for (const PeerId& peer : peer_list) {
    RaftNode::instance().addPeerBeforeStart(peer);
  }

  IPC::barrier(PEERS_SETUP, kProcesses - 1);
  RaftNode::instance().start();
  RaftNode::TimePoint t_begin = std::chrono::system_clock::now();
  uint num_appends = 0;
  while (true) {
    RaftNode::TimePoint now = std::chrono::system_clock::now();
    uint16_t duration_ms = static_cast<uint16_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now - t_begin)
            .count());

    if (RaftNode::instance().state() == RaftNode::State::LEADER) {
      if (duration_ms > 20) {
        RaftNode::instance().appendLogEntry(19);
        ++num_appends;
        t_begin = std::chrono::system_clock::now();
      }
      if (num_appends > 100) {
        RaftNode::instance().giveUpLeadership();
        num_appends = 0;
        t_begin = std::chrono::system_clock::now();
      }
    } else {
      t_begin = std::chrono::system_clock::now();
    }
    // usleep (20000);
  }
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
