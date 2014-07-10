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
    TestChordIndex::instance().staticInit();
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

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
