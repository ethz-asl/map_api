#ifndef MAP_API_CONSENSUS_FIXTURE_INL_H_
#define MAP_API_CONSENSUS_FIXTURE_INL_H_

#include <set>

#include <gtest/gtest.h>

#include <map-api/core.h>
#include <map-api/net-table-manager.h>
#include <map-api/raft-node.h>

constexpr int kTableFieldId = 0;

namespace map_api {

void ConsensusFixture::appendEntries() {
  RaftNode::TimePoint append_time = std::chrono::system_clock::now();
  RaftNode::TimePoint begin_time = std::chrono::system_clock::now();
  double duration_run_ms = 0;

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
        RaftNode::instance().leaderAppendLogEntry(19);
        append_time = std::chrono::system_clock::now();
      }
    } else {
      append_time = std::chrono::system_clock::now();
    }
    // usleep (20000);
  }
}

void ConsensusFixture::SetUpImpl() {
  map_api::Core::initializeInstance();  // Core init.
  ASSERT_TRUE(map_api::Core::instance() != nullptr);
}

void ConsensusFixture::TearDownImpl() { map_api::Core::instance()->kill(); }

}  // namespace map_api

#endif  // MAP_API_CONSENSUS_FIXTURE_INL_H_
