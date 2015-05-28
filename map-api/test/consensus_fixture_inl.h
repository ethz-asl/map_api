#ifndef MAP_API_CONSENSUS_FIXTURE_INL_H_
#define MAP_API_CONSENSUS_FIXTURE_INL_H_

#include <set>

#include <gtest/gtest.h>

#include <map-api/core.h>
#include <map-api/net-table-manager.h>

#include "./raft.pb.h"
#include "map-api/raft-chunk.h"
#include "map-api/raft-chunk-data-ram-container.h"

constexpr int kTableFieldId = 0;

namespace map_api {

void ConsensusFixture::SetUpImpl() {
  map_api::Core::initializeInstance();  // Core init.
  ASSERT_TRUE(map_api::Core::instance() != nullptr);

  // Create a table
  std::shared_ptr<map_api::TableDescriptor> descriptor(new TableDescriptor);
  descriptor->setName("Table0");
  descriptor->addField<int>(kTableFieldId);
  table_ = map_api::NetTableManager::instance().addTable(descriptor);
}

void ConsensusFixture::setupRaftSupervisor(uint64_t num_processes) {
  enum Barriers {
    INIT = 253,  // Some large number.
    SUPERVISOR_ID_ANNOUNCED = 254,
    PEERS_SETUP = 255
  };
  for (uint64_t i = 1u; i <= num_processes; ++i) {
    launchSubprocess(i);
  }
  IPC::barrier(INIT, num_processes);
  IPC::push(PeerId::self());
  IPC::barrier(SUPERVISOR_ID_ANNOUNCED, num_processes);
  IPC::barrier(PEERS_SETUP, num_processes);
}

void ConsensusFixture::setupRaftPeers(uint64_t num_processes) {
  enum Barriers {
    INIT = 253,  // Some large number.
    SUPERVISOR_ID_ANNOUNCED = 254,
    PEERS_SETUP = 255
  };

  IPC::barrier(INIT, num_processes);
  IPC::barrier(SUPERVISOR_ID_ANNOUNCED, num_processes);
  std::set<PeerId> peer_list;
  Hub::instance().getPeers(&peer_list);
  PeerId supervisor = IPC::pop<PeerId>();
  peer_list.erase(supervisor);

  for (const PeerId& peer : peer_list) {
    addRaftPeer(peer);
  }
  IPC::barrier(PEERS_SETUP, num_processes);
}

void ConsensusFixture::addRaftPeer(const PeerId& peer) {
  // RaftNode::instance().addPeerBeforeStart(peer);
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

const PeerId& ConsensusFixture::getLockHolder(RaftChunk* chunk) {
  return chunk->getLockHolder();
}

void ConsensusFixture::quitRaftUnannounced(RaftChunk* chunk) {
  chunk->raft_node_.stop();
}

void ConsensusFixture::leaderAppendBlankLogEntry(RaftChunk* chunk) {
  std::shared_ptr<proto::RaftLogEntry> entry(new proto::RaftLogEntry);
  entry->set_sender(PeerId::self().ipPort());
  entry->set_sender_serial_id(chunk->request_id_.getNewId());
  do {
    CHECK(chunk->raft_node_.getState() == RaftNode::State::LEADER);
  } while (chunk->raft_node_.leaderAppendLogEntry(entry) == 0);
}

void ConsensusFixture::leaderWaitUntilAllCommitted(RaftChunk* chunk) {
  uint64_t term;
  uint64_t last_index;
  bool is_leader;
  {
    std::lock_guard<std::mutex> state_lock(chunk->raft_node_.state_mutex_);
    is_leader = (chunk->raft_node_.state_ == RaftNode::State::LEADER);
    term = chunk->raft_node_.current_term_;
    last_index = chunk->raft_node_.data_->lastLogIndex();
  }
  CHECK(is_leader);
  chunk->raft_node_.waitAndCheckCommit(last_index, term, 0);
}

uint64_t ConsensusFixture::getLatestEntrySerialId(RaftChunk* chunk,
                                              const PeerId& peer) {
  RaftChunkDataRamContainer::LogReadAccess log_reader(chunk->raft_node_.data_);
  return log_reader->getPeerLatestSerialId(peer);
}

void ConsensusFixture::TearDownImpl() { map_api::Core::instance()->kill(); }

}  // namespace map_api

#endif  // MAP_API_CONSENSUS_FIXTURE_INL_H_
