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
/*
TEST_F(ConsensusFixture, LeaderElection) {
  const uint64_t kProcesses = 5;  // Processes in addition to supervisor.
  enum Barriers {
    DIE
  };
  pid_t pid = getpid();

  if (getSubprocessId() == 0) {
    VLOG(1) << "Supervisor Id: " << RaftNode::instance().self_id() << " : PID "
            << pid;
    setupRaftSupervisor(kProcesses);

    IPC::barrier(DIE, kProcesses);
    std::set<PeerId> peer_list;
    Hub::instance().getPeers(&peer_list);
    std::string leader_id;
    // Check if all peers agree on which peer is the leader.
    for (const PeerId& peer : peer_list) {
      proto::QueryStateResponse state = queryState(peer);
      if (leader_id.empty()) {
        leader_id = state.leader_id_();
      }
      EXPECT_EQ(state.leader_id_().compare(leader_id), 0);
    }
  } else {
    VLOG(1) << "Peer Id " << RaftNode::instance().self_id() << " : PID " << pid;
    setupRaftPeers(kProcesses);
    RaftNode::instance().start();
    usleep(5000 * kMillisecondsToMicroseconds);
    RaftNode::instance().stop();
    IPC::barrier(DIE, kProcesses);
  }
}

TEST_F(ConsensusFixture, LeaderChange) {
  const uint64_t kProcesses = 5;  // Processes in addition to supervisor.
  enum Barriers {
    LEADER_ELECTED,
    ELECTION_1_TESTED,
    DIE
  };
  pid_t pid = getpid();

  if (getSubprocessId() == 0) {
    VLOG(1) << "Supervisor Id: " << RaftNode::instance().self_id() << " : PID "
            << pid;
    setupRaftSupervisor(kProcesses);

    IPC::barrier(LEADER_ELECTED, kProcesses);

    std::set<PeerId> peer_list;
    Hub::instance().getPeers(&peer_list);
    std::string leader_id;
    // Check if all peers agree on which peer is the leader.
    for (const PeerId& peer : peer_list) {
      proto::QueryStateResponse state = queryState(peer);
      if (leader_id.empty()) {
        leader_id = state.leader_id_();
      }
      EXPECT_EQ(state.leader_id_().compare(leader_id), 0);
    }
    IPC::barrier(ELECTION_1_TESTED, kProcesses);
    IPC::barrier(DIE, kProcesses);
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
    setupRaftPeers(kProcesses);
    RaftNode::instance().start();
    usleep(1000 * kMillisecondsToMicroseconds);
    IPC::barrier(LEADER_ELECTED, kProcesses);
    IPC::barrier(ELECTION_1_TESTED, kProcesses);

    if (RaftNode::instance().state() == RaftNode::State::LEADER) {
      giveUpLeadership();
    }
    usleep(2000 * kMillisecondsToMicroseconds);
    RaftNode::instance().stop();
    IPC::barrier(DIE, kProcesses);
  }
}

TEST_F(ConsensusFixture, AppendEntries) {
  const uint64_t kProcesses = 5;  // Processes in addition to supervisor.
  enum Barriers {
    LEADER_ELECTED,
    DIE
  };
  pid_t pid = getpid();

  if (getSubprocessId() == 0) {
    VLOG(1) << "Supervisor Id: " << RaftNode::instance().self_id() << " : PID "
            << pid;
    setupRaftSupervisor(kProcesses);

    IPC::barrier(LEADER_ELECTED, kProcesses);
    IPC::barrier(DIE, kProcesses);

    std::vector<proto::QueryStateResponse> states;
    std::set<PeerId> peer_list;
    Hub::instance().getPeers(&peer_list);
    uint64_t max_commit_index = 0;
    for (const PeerId& peer : peer_list) {
      proto::QueryStateResponse state = queryState(peer);
      states.push_back(state);
      EXPECT_EQ(state.commit_index() * kRaftTestAppendEntry,
                state.commit_result());
      if (state.commit_index() > max_commit_index) {
        max_commit_index = state.commit_index();
      }
    }
    // Number of peers with more logs than commit index.
    uint num_complete_log_peers = 0;

    for (proto::QueryStateResponse state : states) {
      if (state.last_log_index() >= max_commit_index) {
        ++num_complete_log_peers;
      }
    }
    // Check if committed entries are replicated on atleast half of the peers.
    EXPECT_GT(num_complete_log_peers, peer_list.size() / 2);

  } else {
    VLOG(1) << "Peer Id " << RaftNode::instance().self_id() << " : PID " << pid;
    setupRaftPeers(kProcesses);

    RaftNode::instance().start();
    usleep(1000 * kMillisecondsToMicroseconds);
    IPC::barrier(LEADER_ELECTED, kProcesses);

    appendEntriesWithLeaderChangesFor(10000, 20);

    IPC::barrier(DIE, kProcesses);
    RaftNode::instance().kill();
  }
}

TEST_F(ConsensusFixture, BurstAppendEntries) {
  const uint64_t kProcesses = 5;  // Processes in addition to supervisor.
  enum Barriers {
    DIE
  };
  pid_t pid = getpid();

  if (getSubprocessId() == 0) {
    VLOG(1) << "Supervisor Id: " << RaftNode::instance().self_id() << " : PID "
            << pid;
    setupRaftSupervisor(kProcesses);
    IPC::barrier(DIE, kProcesses);

    std::set<PeerId> peer_list;
    std::vector<proto::QueryStateResponse> states;
    Hub::instance().getPeers(&peer_list);
    uint64_t max_commit_index = 0;
    for (const PeerId& peer : peer_list) {
      proto::QueryStateResponse state = queryState(peer);
      states.push_back(state);
      EXPECT_EQ(state.commit_index() * kRaftTestAppendEntry,
                state.commit_result());
      if (state.commit_index() > max_commit_index) {
        max_commit_index = state.commit_index();
      }
      VLOG(1) << peer << ": Commit index = " << state.commit_index();
    }
    // Number of peers with more logs than commit index.
    uint num_complete_log_peers = 0;
    for (proto::QueryStateResponse state : states) {
      if (state.last_log_index() >= max_commit_index) {
        ++num_complete_log_peers;
      }
    }
    // Check if committed entries are replicated on atleast half of the peers.
    EXPECT_GT(num_complete_log_peers, peer_list.size() / 2);
  } else {
    VLOG(1) << "Peer Id " << RaftNode::instance().self_id() << " : PID " << pid;
    setupRaftPeers(kProcesses);
    RaftNode::instance().start();
    usleep(1000 * kMillisecondsToMicroseconds);
    appendEntriesBurst(40);
    usleep(4000 * kMillisecondsToMicroseconds);
    RaftNode::instance().stop();
    IPC::barrier(DIE, kProcesses);
  }
}*/

TEST_F(ConsensusFixture, OnePeerJoin) {
  const uint64_t kProcesses = 2;  // Processes in addition to supervisor.
  enum {
    SUPERVISOR,
    LEADER,
    JOINING_PEER
  };
  enum Barriers {
    INIT,
    LEADER_ENTRIES_ADD,
    INIT_JOINING_PEER,
    DIE
  };
  pid_t pid = getpid();

  if (getSubprocessId() == SUPERVISOR) {
    VLOG(1) << "Supervisor Id: " << RaftNode::instance().self_id() << " : PID "
            << pid;
    launchSubprocess(LEADER);
    launchSubprocess(JOINING_PEER);
    IPC::barrier(INIT, kProcesses);
    IPC::barrier(LEADER_ENTRIES_ADD, kProcesses);
    IPC::barrier(INIT_JOINING_PEER, kProcesses);

    IPC::barrier(DIE, kProcesses);

    std::set<PeerId> peer_list;
    std::vector<proto::QueryStateResponse> states;
    Hub::instance().getPeers(&peer_list);
    uint64_t max_commit_index = 0;
    for (const PeerId& peer : peer_list) {
      proto::QueryStateResponse state = queryState(peer);
      states.push_back(state);

      if (state.commit_index() > max_commit_index) {
        max_commit_index = state.commit_index();
      }
      VLOG(1) << peer << ": Commit index = " << state.commit_index();
    }
    // Number of peers with more logs than commit index.
    uint num_complete_log_peers = 0;
    for (proto::QueryStateResponse state : states) {
      if (state.last_log_index() >= max_commit_index) {
        ++num_complete_log_peers;
      }
    }
    // Check if committed entries are replicated on atleast half of the peers.
    EXPECT_GT(num_complete_log_peers, peer_list.size() / 2);
  } else if (getSubprocessId() == LEADER) {
    IPC::barrier(INIT, kProcesses);

    VLOG(1) << "Peer Id " << RaftNode::instance().self_id() << " : PID " << pid;
    RaftNode::instance().start();
    usleep(1000 * kMillisecondsToMicroseconds);
    appendEntriesBurst(40);
    usleep(4000 * kMillisecondsToMicroseconds);
    IPC::push(PeerId::self());
    IPC::barrier(LEADER_ENTRIES_ADD, kProcesses);
    IPC::barrier(INIT_JOINING_PEER, kProcesses);

    usleep(8000 * kMillisecondsToMicroseconds);
    IPC::barrier(DIE, kProcesses);
    RaftNode::instance().stop();

  } else if (getSubprocessId() == JOINING_PEER) {
    IPC::barrier(INIT, kProcesses);
    VLOG(1) << "Peer Id " << RaftNode::instance().self_id() << " : PID " << pid;
    IPC::barrier(LEADER_ENTRIES_ADD, kProcesses);
    setJoinRequestPeer(IPC::pop<PeerId>());
    RaftNode::instance().start();
    IPC::barrier(INIT_JOINING_PEER, kProcesses);
    IPC::barrier(DIE, kProcesses);
    RaftNode::instance().stop();
  }
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
