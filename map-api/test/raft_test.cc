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

TEST_F(ConsensusFixture, LonePeer) {
  const uint64_t kProcesses = 2;
  enum Barriers {
    INIT,
    PEERS_SETUP,
    APPENDED_ENTRIES,
    NEW_PEER,
    DIE
  };

  pid_t pid = getpid();
  VLOG(1) << "Peer Id " << RaftNode::instance().self_id() << " : PID " << pid;

  if (getSubprocessId() == 0) {
    for (uint64_t i = 1u; i < kProcesses; ++i) {
      launchSubprocess(i);
    }
    RaftNode::instance().registerHandlers();

    IPC::barrier(INIT, kProcesses - 1);
    // Find peers in the network and add them to raft cluster.
    std::set<PeerId> peer_list;
    Hub::instance().getPeers(&peer_list);
    for (const PeerId& peer : peer_list) {
      // RaftNode::instance().addPeerBeforeStart(peer);
    }

    IPC::barrier(PEERS_SETUP, kProcesses - 1);
    RaftNode::instance().start();
    RaftNode::TimePoint append_time = std::chrono::system_clock::now();
    RaftNode::TimePoint begin_time = std::chrono::system_clock::now();

    double duration_run_ms = 0;
    uint num_appends = 0;

    while (duration_run_ms < 2000) {
      RaftNode::TimePoint now = std::chrono::system_clock::now();
      uint16_t duration_ms = static_cast<uint16_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              now - append_time).count());
      duration_run_ms = static_cast<double>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              now - begin_time).count());

      if (RaftNode::instance().state() == RaftNode::State::LEADER) {
        if (duration_ms > 15) {
          VLOG_EVERY_N(1, 1000) << PeerId::self() << " Is the leader.";
          RaftNode::instance().leaderAppendLogEntry(19);
          ++num_appends;
          append_time = std::chrono::system_clock::now();
        }
      } else {
        append_time = std::chrono::system_clock::now();
      }
      // usleep (20000);
    }

    VLOG(1) << "creating new peer";
    IPC::push<std::string>(PeerId::self().ipPort());
    IPC::barrier(APPENDED_ENTRIES, kProcesses - 1);
    IPC::barrier(NEW_PEER, kProcesses - 1);

    VLOG(1) << "Run 2";

    append_time = std::chrono::system_clock::now();
    begin_time = std::chrono::system_clock::now();

    duration_run_ms = 0;
    while (duration_run_ms < 2000) {
      RaftNode::TimePoint now = std::chrono::system_clock::now();
      uint16_t duration_ms = static_cast<uint16_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              now - append_time).count());
      duration_run_ms = static_cast<double>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              now - begin_time).count());

      if (RaftNode::instance().state() == RaftNode::State::LEADER) {
        if (duration_ms > 15) {
          VLOG_EVERY_N(1, 1000) << PeerId::self() << " Is the leader.";
          RaftNode::instance().leaderAppendLogEntry(19);
          ++num_appends;
          append_time = std::chrono::system_clock::now();
        }
      } else {
        append_time = std::chrono::system_clock::now();
      }
      // usleep (20000);
    }

    IPC::barrier(DIE, kProcesses - 1);
    VLOG(1) << "Leader Die. ";
    RaftNode::instance().kill();

  } else {
    IPC::barrier(INIT, kProcesses - 1);
    IPC::barrier(PEERS_SETUP, kProcesses - 1);
    IPC::barrier(APPENDED_ENTRIES, kProcesses - 1);

    RaftNode::instance().initial_join_request_peer_ =
        PeerId(IPC::pop<std::string>());
    RaftNode::instance().state_ = RaftNode::State::JOINING;

    RaftNode::instance().registerHandlers();
    RaftNode::instance().start();
    IPC::barrier(NEW_PEER, kProcesses - 1);

    IPC::barrier(DIE, kProcesses - 1);
    VLOG(1) << "Follower Die. ";
    RaftNode::instance().kill();
  }
}

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
      if (duration_ms > 15) {
        RaftNode::instance().leaderAppendLogEntry(19);
        ++num_appends;
        t_begin = std::chrono::system_clock::now();
      }
      if (num_appends > 200) {
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
