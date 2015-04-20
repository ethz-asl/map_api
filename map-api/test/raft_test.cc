#include <set>
#include <sys/types.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent-mapping-common/conversions.h>

#include "map-api/hub.h"
#include "map-api/ipc.h"
#include "map-api/peer-id.h"
#include "map-api/raft-node.h"
#include "map-api/test/testing-entrypoint.h"
#include "./consensus_fixture.h"

namespace map_api {

TEST_F(ConsensusFixture, LeaderElection) {
  const uint64_t kProcesses = 6;
  enum Barriers {
    INIT,
    PEERS_SETUP,
    SUPERVISOR_ID_ANNOUNCED,
    DIE
  };
  pid_t pid = getpid();

  if (getSubprocessId() == 0) {
    VLOG(1) << "Supervisor Id: " << RaftNode::instance().self_id() << " : PID "
            << pid;
    setupSupervisor(kProcesses);

    IPC::barrier(DIE, kProcesses - 1);
    std::set<PeerId> peer_list;
    Hub::instance().getPeers(&peer_list);
    std::string leader_id;
    for (const PeerId& peer : peer_list) {
      proto::QueryStateResponse state = queryState(peer);
      if (leader_id.empty()) {
        leader_id = state.leader_id_();
      }
      EXPECT_EQ(state.leader_id_().compare(leader_id), 0);
    }
  } else {
    VLOG(1) << "Peer Id " << RaftNode::instance().self_id() << " : PID " << pid;
    setupPeers(kProcesses);
    RaftNode::instance().start();
    usleep(5000 * kMillisecondsToMicroseconds);
    RaftNode::instance().stop();
    IPC::barrier(DIE, kProcesses - 1);
  }
}

TEST_F(ConsensusFixture, LeaderChange) {
  const uint64_t kProcesses = 6;
  enum Barriers {
    INIT,
    PEERS_SETUP,
    SUPERVISOR_ID_ANNOUNCED,
    LEADER_ELECTED,
    ELECTION_1_TESTED,
    DIE
  };
  pid_t pid = getpid();

  if (getSubprocessId() == 0) {
    VLOG(1) << "Supervisor Id: " << RaftNode::instance().self_id() << " : PID "
            << pid;
    setupSupervisor(kProcesses);

    IPC::barrier(LEADER_ELECTED, kProcesses - 1);

    std::set<PeerId> peer_list;
    Hub::instance().getPeers(&peer_list);
    std::string leader_id;
    for (const PeerId& peer : peer_list) {
      proto::QueryStateResponse state = queryState(peer);
      if (leader_id.empty()) {
        leader_id = state.leader_id_();
      }
      EXPECT_EQ(state.leader_id_().compare(leader_id), 0);
    }
    IPC::barrier(ELECTION_1_TESTED, kProcesses - 1);
    IPC::barrier(DIE, kProcesses - 1);
    leader_id.clear();
    for (const PeerId& peer : peer_list) {
      proto::QueryStateResponse state = queryState(peer);
      if (leader_id.empty()) {
        leader_id = state.leader_id_();
      }
      EXPECT_EQ(state.leader_id_().compare(leader_id), 0);
    }
  } else {
    VLOG(1) << "Peer Id " << RaftNode::instance().self_id() << " : PID " << pid;
    setupPeers(kProcesses);

    RaftNode::instance().start();
    usleep(1000 * kMillisecondsToMicroseconds);
    IPC::barrier(LEADER_ELECTED, kProcesses - 1);

    IPC::barrier(ELECTION_1_TESTED, kProcesses - 1);
    if (RaftNode::instance().state() == RaftNode::State::LEADER) {
      giveUpLeadership();
    }
    usleep(2000 * kMillisecondsToMicroseconds);

    RaftNode::instance().stop();

    IPC::barrier(DIE, kProcesses - 1);
  }
}

TEST_F(ConsensusFixture, AppendEntries) {
  const uint64_t kProcesses = 6;
  enum Barriers {
    INIT,
    PEERS_SETUP,
    SUPERVISOR_ID_ANNOUNCED,
    LEADER_ELECTED,
    DIE
  };
  pid_t pid = getpid();

  if (getSubprocessId() == 0) {
    VLOG(1) << "Supervisor Id: " << RaftNode::instance().self_id() << " : PID "
            << pid;
    setupSupervisor(kProcesses);

    IPC::barrier(LEADER_ELECTED, kProcesses - 1);
    IPC::barrier(DIE, kProcesses - 1);

    // TODO(aqurai): Complete this test after fixing conflict issue.

  } else {
    VLOG(1) << "Peer Id " << RaftNode::instance().self_id() << " : PID " << pid;
    setupPeers(kProcesses);

    RaftNode::instance().start();
    usleep(1000 * kMillisecondsToMicroseconds);
    IPC::barrier(LEADER_ELECTED, kProcesses - 1);

    uint num_appends = 0;
    TimePoint begin = std::chrono::system_clock::now();
    TimePoint append_time = std::chrono::system_clock::now();
    uint16_t total_duration_ms = 0;
    while (total_duration_ms < 5000) {
      TimePoint now = std::chrono::system_clock::now();
      uint16_t append_duration_ms = static_cast<uint16_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              now - append_time).count());

      // Append new entries if leader.
      if (RaftNode::instance().state() == RaftNode::State::LEADER) {
        if (append_duration_ms > 20) {
          appendEntry();
          ++num_appends;
          append_time = std::chrono::system_clock::now();
        }

        if (num_appends > 100) {
          giveUpLeadership();
          num_appends = 0;
          append_time = std::chrono::system_clock::now();
        }
      } else {
        append_time = std::chrono::system_clock::now();
      }

      total_duration_ms = static_cast<uint16_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(now - begin)
              .count());
    }

    IPC::barrier(DIE, kProcesses - 1);
  }
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
