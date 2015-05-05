#include <set>
#include <sys/types.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent-mapping-common/conversions.h>

#include "map-api/hub.h"
#include "map-api/ipc.h"
#include "map-api/peer-id.h"
#include "map-api/raft-node.h"
#include "map-api/raft-chunk.h"
#include "map-api/test/testing-entrypoint.h"
#include "./consensus_fixture.h"

namespace map_api {

constexpr int kWaitTimeMs = 1000;
constexpr int kAppendEntriesForMs = 10000;
constexpr int kTimeBetweenAppendsMs = 20;
constexpr int kNumEntriesToAppend = 40;

TEST_F(ConsensusFixture, RaftChunkTest) {
  const uint64_t kProcesses = 5;
  enum Barriers {
    INIT_PEERS,
    PUSH_CHUNK_ID,
    RAFT_START,
    DIE
  };
  pid_t pid = getpid();
  VLOG(1) << "PID: " << pid << ", IP: " << PeerId::self();
  if (getSubprocessId() == 0) {
    for (uint64_t i = 1u; i < kProcesses; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    ChunkBase* base_chunk = table_->newChunk();
    RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    CHECK_NOTNULL(chunk);
    IPC::push(chunk->id());
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);
    usleep(5000000);
    // chunk->raft_node_.start();
    std::set<PeerId> peers;
    Hub::instance().getPeers(&peers);
    for (const PeerId& peer : peers) {
      chunk->requestParticipation(peer);
    }
    IPC::barrier(RAFT_START, kProcesses - 1);
    usleep(100000 * kMillisecondsToMicroseconds);

    IPC::barrier(DIE, kProcesses - 1);
  } else {
    IPC::barrier(INIT_PEERS, kProcesses - 1);
    IPC::barrier(PUSH_CHUNK_ID, kProcesses - 1);
    common::Id chunk_id = IPC::pop<common::Id>();
    // ChunkBase* base_chunk = table_->getChunk(chunk_id);
    // RaftChunk* chunk = dynamic_cast<RaftChunk*>(base_chunk);
    // CHECK_NOTNULL(chunk);
    // chunk->raft_node_.start();
    IPC::barrier(RAFT_START, kProcesses - 1);

    usleep(100000 * kMillisecondsToMicroseconds);

    IPC::barrier(DIE, kProcesses - 1);
  }
}
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
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
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
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    IPC::barrier(LEADER_ELECTED, kProcesses);
    IPC::barrier(ELECTION_1_TESTED, kProcesses);

    if (RaftNode::instance().state() == RaftNode::State::LEADER) {
      giveUpLeadership();
    }
    usleep(2 * kWaitTimeMs * kMillisecondsToMicroseconds);
    RaftNode::instance().stop();
    IPC::barrier(DIE, kProcesses);
  }
}

TEST_F(ConsensusFixture, AppendEntriesWithLeaderChange) {
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
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    IPC::barrier(LEADER_ELECTED, kProcesses);

    appendEntriesWithLeaderChangesForMs(kAppendEntriesForMs,
                                        kTimeBetweenAppendsMs);

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
    // Number of peers with more logs than commit index. An entry should be
    // committed only if it is replicated on majority of the peers.
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
    usleep(kWaitTimeMs * kMillisecondsToMicroseconds);
    appendEntriesBurst(kNumEntriesToAppend);
    usleep(4 * kWaitTimeMs * kMillisecondsToMicroseconds);
    RaftNode::instance().stop();
    IPC::barrier(DIE, kProcesses);
  }
} */

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
