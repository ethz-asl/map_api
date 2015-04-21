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
  RaftNode::instance().addPeerBeforeStart(peer);
}

void ConsensusFixture::appendEntriesFor(uint16_t duration_ms,
                                        uint16_t delay_ms) {
  TimePoint begin = std::chrono::system_clock::now();
  TimePoint append_time = std::chrono::system_clock::now();
  uint16_t total_duration_ms = 0;
  while (total_duration_ms < duration_ms) {
    TimePoint now = std::chrono::system_clock::now();
    uint16_t append_duration_ms = static_cast<uint16_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now - append_time)
            .count());

    // Append new entries if leader.
    if (RaftNode::instance().state() == RaftNode::State::LEADER) {
      if (append_duration_ms > delay_ms) {
        appendEntry();
        append_time = std::chrono::system_clock::now();
      }
      usleep(delay_ms * kMillisecondsToMicroseconds);
    } else {
      append_time = std::chrono::system_clock::now();
    }

    total_duration_ms = static_cast<uint16_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now - begin)
            .count());
  }
}

void ConsensusFixture::appendEntriesWithLeaderChangesFor(uint16_t duration_ms,
                                                         uint16_t delay_ms) {
  uint num_appends = 0;
  TimePoint begin = std::chrono::system_clock::now();
  TimePoint append_time = std::chrono::system_clock::now();
  uint16_t total_duration_ms = 0;
  while (total_duration_ms < duration_ms) {
    TimePoint now = std::chrono::system_clock::now();
    uint16_t append_duration_ms = static_cast<uint16_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now - append_time)
            .count());

    total_duration_ms = static_cast<uint16_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now - begin)
            .count());
    // Append new entries if leader.
    if (RaftNode::instance().state() == RaftNode::State::LEADER) {
      if (append_duration_ms > delay_ms) {
        appendEntry();
        ++num_appends;
        append_time = std::chrono::system_clock::now();
      }

      if (num_appends > 100) {
        giveUpLeadership();
        num_appends = 0;
        append_time = std::chrono::system_clock::now();
      }
      usleep(delay_ms * kMillisecondsToMicroseconds);
    } else {
      append_time = std::chrono::system_clock::now();
    }
  }
}

void ConsensusFixture::appendEntriesBurst(uint16_t num_entries) {
  if (RaftNode::instance().state() != RaftNode::State::LEADER) {
    return;
  }
  for (uint16_t k = 0; k < num_entries; ++k) {
    appendEntry();
  }
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
