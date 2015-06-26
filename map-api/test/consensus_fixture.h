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
  static RaftChunk* createChunkAndPushId(NetTable* table);
  static RaftChunk* getPushedChunk(NetTable* table);

  proto::QueryStateResponse queryState(const PeerId& peer);
  const PeerId& getLockHolder(RaftChunk* chunk);
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
  NetTable* table_;
};

}  // namespace map_api

#include "./consensus_fixture_inl.h"

#endif  // MAP_API_CONSENSUS_FIXTURE_H_
