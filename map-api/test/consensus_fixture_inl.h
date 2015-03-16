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
  std::unique_ptr<map_api::TableDescriptor> descriptor;

  // Create a table
  descriptor.reset(new map_api::TableDescriptor);
  descriptor->setName("Table0");
  descriptor->addField<int>(kTableFieldId);
  table_ = map_api::NetTableManager::instance().addTable(
      map_api::CRTable::Type::CRU, &descriptor);
}

void ConsensusFixture::TearDownImpl() { map_api::Core::instance()->kill(); }

void ConsensusFixture::mainProcess(uint64_t num_sub_processes) {
  num_sub_processes_ = num_sub_processes;
}

/*const std::set<PeerId>& ConsensusFixture::getChunkPeerList(Chunk* chunk) {
  CHECK_NOTNULL(chunk);
  return chunk->peers_.peers();
}*/

}  // namespace map_api

#endif  // MAP_API_CONSENSUS_FIXTURE_INL_H_
