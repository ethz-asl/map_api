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
  
TEST_F(ConsensusFixture, PeerAnnouncedQuit) {
  const uint64_t kProcesses = 2;
  enum Barriers {
    INIT,
    PEERS_SETUP,
    APPENDED_ENTRIES,
    NEW_PEER,
    PEER_QUIT,
    DIE
  };
  
  pid_t pid = getpid();
  if (getSubprocessId() == 0) {
    VLOG(1) << "PID of main process. " << pid;
    for (uint64_t i = 1u; i <= kProcesses; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT, kProcesses);
    IPC::barrier(PEERS_SETUP, kProcesses);
    IPC::barrier(APPENDED_ENTRIES, kProcesses);
    IPC::barrier(NEW_PEER, kProcesses);
    IPC::barrier(PEER_QUIT, kProcesses);
    IPC::barrier(DIE, kProcesses);
    
  } else if (getSubprocessId() == 1) {
    VLOG(1) << "Peer Id " << RaftNode::instance().self_id() << " : PID " << pid; 
    RaftNode::instance().registerHandlers();

    IPC::barrier(INIT, kProcesses);
    // Find peers in the network and add them to raft cluster.
    std::set<PeerId> peer_list;
    Hub::instance().getPeers(&peer_list);
    for (const PeerId& peer : peer_list) {
      // RaftNode::instance().addPeerBeforeStart(peer);
    }

    IPC::barrier(PEERS_SETUP, kProcesses);
    RaftNode::instance().start();
    appendEntries();
    IPC::push<std::string>(PeerId::self().ipPort());
    IPC::barrier(APPENDED_ENTRIES, kProcesses);
    VLOG(1) << "Leader: Waiting for new peer to join.";
    IPC::barrier(NEW_PEER, kProcesses);

    VLOG(1) << "Leader: New peer has joined.";

    appendEntries();
    IPC::barrier(PEER_QUIT, kProcesses);
    VLOG(1) << " Starting while true. ";
    while (true) {
      VLOG_EVERY_N(1, 1) << "Adding entry ... ";
      appendEntries();
    }
    
    //IPC::barrier(DIE, kProcesses);
    VLOG(1) << "Leader Die. ";
    RaftNode::instance().kill();
  } else {
    VLOG(1) << "Peer Id " << RaftNode::instance().self_id() << " : PID " << pid; 
    IPC::barrier(INIT, kProcesses);
    IPC::barrier(PEERS_SETUP, kProcesses);
    IPC::barrier(APPENDED_ENTRIES, kProcesses);

    RaftNode::instance().initial_join_request_peer_ =
        PeerId(IPC::pop<std::string>());
    RaftNode::instance().state_ = RaftNode::State::JOINING;

    RaftNode::instance().registerHandlers();
    RaftNode::instance().start();
    IPC::barrier(NEW_PEER, kProcesses);
    
    IPC::barrier(PEER_QUIT, kProcesses);
    

    //IPC::barrier(DIE, kProcesses);
    VLOG(1) << "Follower Die. ";
    RaftNode::instance().kill();
    CHECK(false);
  }
}

TEST_F(ConsensusFixture, PeerJoin) {
  const uint64_t kProcesses = 2;
  enum Barriers {
    INIT,
    PEERS_SETUP,
    APPENDED_ENTRIES,
    NEW_PEER,
    DIE
  };
  
  pid_t pid = getpid();
  if (getSubprocessId() == 0) {
    VLOG(1) << "PID of main process. " << pid;
    for (uint64_t i = 1u; i <= kProcesses; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT, kProcesses);
    IPC::barrier(PEERS_SETUP, kProcesses);
    IPC::barrier(APPENDED_ENTRIES, kProcesses);
    IPC::barrier(NEW_PEER, kProcesses);
    IPC::barrier(DIE, kProcesses);
    
  } else if (getSubprocessId() == 1) {
    VLOG(1) << "Peer Id " << RaftNode::instance().self_id() << " : PID " << pid; 
    RaftNode::instance().registerHandlers();

    IPC::barrier(INIT, kProcesses);
    // Find peers in the network and add them to raft cluster.
    std::set<PeerId> peer_list;
    Hub::instance().getPeers(&peer_list);
    for (const PeerId& peer : peer_list) {
      // RaftNode::instance().addPeerBeforeStart(peer);
    }

    IPC::barrier(PEERS_SETUP, kProcesses);
    RaftNode::instance().start();
    
    appendEntries();

    VLOG(1) << "creating new peer";
    IPC::push<std::string>(PeerId::self().ipPort());
    IPC::barrier(APPENDED_ENTRIES, kProcesses);
    IPC::barrier(NEW_PEER, kProcesses);

    VLOG(1) << "Run 2";

    appendEntries();

    IPC::barrier(DIE, kProcesses);
    VLOG(1) << "Leader Die. ";
    RaftNode::instance().kill();
  } else {
    VLOG(1) << "Peer Id " << RaftNode::instance().self_id() << " : PID " << pid; 
    IPC::barrier(INIT, kProcesses);
    IPC::barrier(PEERS_SETUP, kProcesses);
    IPC::barrier(APPENDED_ENTRIES, kProcesses);

    RaftNode::instance().initial_join_request_peer_ =
        PeerId(IPC::pop<std::string>());
    RaftNode::instance().state_ = RaftNode::State::JOINING;

    RaftNode::instance().registerHandlers();
    RaftNode::instance().start();
    IPC::barrier(NEW_PEER, kProcesses);

    IPC::barrier(DIE, kProcesses);
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
  while (true) {
    // Do nothing
  }
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
