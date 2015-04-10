#ifndef MAP_API_CONSENSUS_FIXTURE_INL_H_
#define MAP_API_CONSENSUS_FIXTURE_INL_H_

#include <set>

#include <gtest/gtest.h>

#include <map-api/core.h>
#include <map-api/net-table-manager.h>

#include "./raft.pb.h"

constexpr int kTableFieldId = 0;

namespace map_api {

void ConsensusFixture::SetUpImpl() {
  map_api::Core::initializeInstance();  // Core init.
  ASSERT_TRUE(map_api::Core::instance() != nullptr);
  RaftNode::instance().registerHandlers();
}

void ConsensusFixture::setupSupervisor(uint64_t num_processes) {
  enum Barriers {
    INIT = 253,  // Some large number.
    SUPERVISOR_ID_ANNOUNCED = 254,
    PEERS_SETUP = 255
  };
  for (uint64_t i = 1u; i < num_processes; ++i) {
    launchSubprocess(i);
  }
  IPC::barrier(INIT, num_processes - 1);
  IPC::push(PeerId::self());
  IPC::barrier(SUPERVISOR_ID_ANNOUNCED, num_processes - 1);
  IPC::barrier(PEERS_SETUP, num_processes - 1);
}

void ConsensusFixture::setupPeers(uint64_t num_processes) {
  enum Barriers {
    INIT = 253,  // Some large number.
    SUPERVISOR_ID_ANNOUNCED = 254,
    PEERS_SETUP = 255
  };

  IPC::barrier(INIT, num_processes - 1);
  IPC::barrier(SUPERVISOR_ID_ANNOUNCED, num_processes - 1);
  std::set<PeerId> peer_list;
  Hub::instance().getPeers(&peer_list);
  PeerId supervisor = IPC::pop<PeerId>();
  peer_list.erase(supervisor);

  for (const PeerId& peer : peer_list) {
    addRaftPeer(peer);
  }
  IPC::barrier(PEERS_SETUP, num_processes - 1);
}

void ConsensusFixture::addRaftPeer(const PeerId& peer) {
  RaftNode::instance().addPeerBeforeStart(peer);
}

proto::QueryStateResponse ConsensusFixture::queryState(const PeerId& peer) {
  Message request, response;
  proto::QueryStateResponse state_response;
  request.impose<RaftNode::kQueryState>();
  if (Hub::instance().try_request(peer, &request, &response)) {
    response.extract<RaftNode::kQueryStateResponse>(&state_response);
  } else {
    LOG(WARNING) << "Supervisor: QueryState request failed for " << peer;
  }
  return state_response;
}

void ConsensusFixture::TearDownImpl() { map_api::Core::instance()->kill(); }

}  // namespace map_api

#endif  // MAP_API_CONSENSUS_FIXTURE_INL_H_
