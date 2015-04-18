#ifndef MAP_API_CONSENSUS_FIXTURE_H_
#define MAP_API_CONSENSUS_FIXTURE_H_

#include <set>

#include <multiprocess-gtest/multiprocess-fixture.h>

#include "map-api/net-table.h"
#include "./net-table.pb.h"

namespace map_api {

class ConsensusFixture : public common::MultiprocessFixture {
 public:
   void appendEntries();
    
 protected:
  virtual void SetUpImpl();
  virtual void TearDownImpl();
};

}  // namespace map_api

#include "./consensus_fixture_inl.h"

#endif  // MAP_API_CONSENSUS_FIXTURE_H_
