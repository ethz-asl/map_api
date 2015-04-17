#ifndef MAP_API_CONSENSUS_FIXTURE_INL_H_
#define MAP_API_CONSENSUS_FIXTURE_INL_H_

#include <set>

#include <gtest/gtest.h>

#include <map-api/core.h>
#include <map-api/net-table-manager.h>

constexpr int kTableFieldId = 0;

namespace map_api {

void ConsensusFixture::SetUpImpl() {
  map_api::Core::initializeInstance();  // Core init.
  ASSERT_TRUE(map_api::Core::instance() != nullptr);
}

void ConsensusFixture::TearDownImpl() { map_api::Core::instance()->kill(); }

}  // namespace map_api

#endif  // MAP_API_CONSENSUS_FIXTURE_INL_H_
