#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "map-api/map-api-core.h"

#include "test_chord_index.cpp"
#include "multiprocess_fixture.cpp"

using namespace map_api;

class ChordIndexTest : public MultiprocessTest {
 protected:
  virtual void SetUp() {
    TestChordIndex::staticInit();
    MapApiCore::instance();
  }
};

TEST_F(ChordIndexTest, create) {
  TestChordIndex::instance().create();
}

DEFINE_uint64(addresses_to_hash, 5, "Amount of addresses to hash");
TEST_F(ChordIndexTest, hashDistribution) {
  enum Barriers{INIT, HASH};
  if (getSubprocessId() == 0) {
    std::ostringstream command_extra;
    command_extra << "--addresses_to_hash=" << FLAGS_addresses_to_hash;
    for (size_t i = 1; i < FLAGS_addresses_to_hash; ++i) {
      launchSubprocess(i, command_extra.str());
    }
    IPC::barrier(INIT, FLAGS_addresses_to_hash - 1);
    std::ostringstream hash_ss;
    hash_ss << "[" << ChordIndex::hash(PeerId::self());
    IPC::barrier(HASH, FLAGS_addresses_to_hash - 1);
    for (size_t i = 1; i < FLAGS_addresses_to_hash; ++i) {
      std::string hash_string;
      IPC::pop(&hash_string);
      hash_ss << ", " << hash_string;
    }
    hash_ss << "]";
    LOG(INFO) << hash_ss.str();
    harvest(false);
  } else {
    IPC::barrier(INIT, FLAGS_addresses_to_hash - 1);
    std::ostringstream hash_ss;
    hash_ss << ChordIndex::hash(PeerId::self());
    IPC::push(hash_ss.str());
    IPC::barrier(HASH, FLAGS_addresses_to_hash - 1);
  }
}

TEST_F(ChordIndexTest, onePeerJoin) {
  constexpr size_t kNProcesses = 3;
  enum Barriers{INIT, ROOT_SHARED, JOINED, SHARED_PEERS};
  if (getSubprocessId() == 0) {
    TestChordIndex::instance().create();
    for (size_t i = 1; i < kNProcesses; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT, kNProcesses - 1);
    IPC::push(PeerId::self().ipPort());
    IPC::barrier(ROOT_SHARED, kNProcesses - 1);
    IPC::barrier(JOINED, kNProcesses - 1);
    std::map<std::string, int> peers;
    ++peers[TestChordIndex::instance().predecessor_->id.ipPort()];
    ++peers[TestChordIndex::instance().successor_->id.ipPort()];
    IPC::barrier(SHARED_PEERS, kNProcesses - 1);
    for (size_t i = 0; i < 2*kNProcesses; ++i) {
      std::string peer;
      IPC::pop(&peer);
      ++peers[peer];
    }
    harvest(false);
    for (const std::pair<std::string, int> peer_count : peers) {
      EXPECT_EQ(2, peer_count.second);
    }
  } else {
    IPC::barrier(INIT, kNProcesses - 1);
    IPC::barrier(ROOT_SHARED, kNProcesses - 1);
    std::string root_string;
    IPC::pop(&root_string);
    PeerId root(root_string);
    TestChordIndex::instance().join(root);
    IPC::barrier(JOINED, kNProcesses - 1);
    IPC::push(TestChordIndex::instance().predecessor_->id.ipPort());
    IPC::push(TestChordIndex::instance().successor_->id.ipPort());
    IPC::barrier(SHARED_PEERS, kNProcesses - 1);
  }
}

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
