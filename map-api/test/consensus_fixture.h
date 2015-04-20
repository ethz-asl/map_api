#ifndef MAP_API_CONSENSUS_FIXTURE_H_
#define MAP_API_CONSENSUS_FIXTURE_H_

#include <set>

#include <multiprocess-gtest/multiprocess-fixture.h>

#include "map-api/net-table.h"
#include "./net-table.pb.h"
#include "./raft.pb.h"

namespace map_api {

constexpr uint32_t kRaftTestAppendEntry = 19;

class ConsensusFixture : public common::MultiprocessFixture {
 public:
  void setupRaftSupervisor(uint64_t num_processes);
  void setupRaftPeers(uint64_t num_processes);

  void addRaftPeer(const PeerId& peer);
  proto::QueryStateResponse queryState(const PeerId& peer);
  void giveUpLeadership() { RaftNode::instance().giveUpLeadership(); }
  void appendEntry() {
    RaftNode::instance().leaderAppendLogEntry(kRaftTestAppendEntry);
  }

  // Keep apeend entries for a duration of duration_ms, with a delay of
  // delay_ms between consecutive appends.
  void appendEntriesFor(uint16_t duration_ms, uint16_t delay_ms);
  void appendEntriesWithLeaderChangesFor(uint16_t duration_ms,
                                         uint16_t delay_ms);

  // Add num_entries entries in a burst
  void appendEntriesBurst(uint16_t num_entries);

  typedef std::chrono::time_point<std::chrono::system_clock> TimePoint;

 protected:
  virtual void SetUpImpl();
  virtual void TearDownImpl();
};

}  // namespace map_api

#include "./consensus_fixture_inl.h"

#endif  // MAP_API_CONSENSUS_FIXTURE_H_
