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
  // Setup supervisor and peers.
  void setupRaftSupervisor(uint64_t num_processes);
  void setupRaftPeers(uint64_t num_processes);

  void addRaftPeer(const PeerId& peer);
  proto::QueryStateResponse queryState(const PeerId& peer);
  void quitRaftUnannounced(RaftChunk* chunk);
  
  void leaderAppendBlankLogEntry(RaftChunk* chunk);
  void leaderWaitUntilAllCommitted(RaftChunk* chunk);
  
  uint64_t getLatestEntrySerialId(RaftChunk* chunk, const PeerId& peer);

  // Keep apeend entries for a duration of duration_ms, with a delay of
  // delay_ms between consecutive appends.
  void appendEntriesForMs(uint16_t duration_ms, uint16_t delay_ms);
  void appendEntriesWithLeaderChangesForMs(uint16_t duration_ms,
                                           uint16_t delay_ms);

  typedef std::chrono::time_point<std::chrono::system_clock> TimePoint;

 protected:
  virtual void SetUpImpl();
  virtual void TearDownImpl();
  uint64_t entry_serial_id_;
  NetTable* table_;
};

}  // namespace map_api

#include "./consensus_fixture_inl.h"

#endif  // MAP_API_CONSENSUS_FIXTURE_H_
