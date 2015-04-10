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
  void setupSupervisor(uint64_t num_processes);
  void setupPeers(uint64_t num_processes);

  void addRaftPeer(const PeerId& peer);
  proto::QueryStateResponse queryState(const PeerId& peer);
  void giveUpLeadership() { RaftNode::instance().giveUpLeadership(); }
  void appendEntry() {
    RaftNode::instance().appendLogEntry(kRaftTestAppendEntry);
  }

  typedef std::chrono::time_point<std::chrono::system_clock> TimePoint;

 protected:
  virtual void SetUpImpl();
  virtual void TearDownImpl();
};

}  // namespace map_api

#include "./consensus_fixture_inl.h"

#endif  // MAP_API_CONSENSUS_FIXTURE_H_
