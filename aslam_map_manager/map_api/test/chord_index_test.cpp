#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>

#include "map-api/map-api-core.h"

#include "test_chord_index.cpp"
#include "multiprocess_fixture.cpp"

DECLARE_uint64(stabilize_us);

namespace map_api {

class ChordIndexTest : public MultiprocessTest {
 protected:
  virtual void SetUp() override {
    // not using MultiprocessTest::SetUp intentionally - need to register
    // handlers first
    TestChordIndex::staticInit();
    MapApiCore::instance();
  }
};

TEST_F(ChordIndexTest, create) {
  TestChordIndex::instance().create();
  TestChordIndex::instance().leave();
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
  } else {
    IPC::barrier(INIT, FLAGS_addresses_to_hash - 1);
    std::ostringstream hash_ss;
    hash_ss << ChordIndex::hash(PeerId::self());
    IPC::push(hash_ss.str());
    IPC::barrier(HASH, FLAGS_addresses_to_hash - 1);
  }
}

class ChordIndexTestInitialized : public ChordIndexTest {
 protected:
  virtual void SetUp() override {
    ChordIndexTest::SetUp();
    if (getSubprocessId() == 0) {
      TestChordIndex::instance().create();
    }
  }

  virtual void TearDown() override {
    TestChordIndex::instance().leave();
    ChordIndexTest::TearDown();
  }

  /**
   * Chord ring must have at least two peers or this will loop forever
   */
  void addNonLocalData(std::string* key, std::string* value, int index) {
    CHECK_NOTNULL(key);
    CHECK_NOTNULL(value);
    std::string result;
    for (size_t salt = 0; ; ++salt) {
      std::ostringstream suffix;
      suffix << index << ":" << salt;
      *key = "key" + suffix.str();
      *value = "value" + suffix.str();
      EXPECT_TRUE(TestChordIndex::instance().addData(*key, *value));
      if (!TestChordIndex::instance().retrieveDataLocally(*key, &result)) {
        break;
      }
    }
  }
};

DEFINE_uint64(chord_processes, 10, "Amount of processes to test chord with");

TEST_F(ChordIndexTestInitialized, onePeerJoin) {
  const size_t kNProcesses = FLAGS_chord_processes;
  enum Barriers{INIT, ROOT_SHARED, JOINED, SHARED_PEERS};
  if (getSubprocessId() == 0) {
    std::ostringstream command_extra;
    command_extra << "--chord_processes=" << FLAGS_chord_processes;
    for (size_t i = 1; i < kNProcesses; ++i) {
      launchSubprocess(i, command_extra.str());
    }
    IPC::barrier(INIT, kNProcesses - 1);
    IPC::push(PeerId::self().ipPort());
    IPC::barrier(ROOT_SHARED, kNProcesses - 1);
    usleep(10 * kNProcesses * FLAGS_stabilize_us); // yes, 10 is a magic number
    // it should be an upper bound of the amount of required stabilization
    // iterations per process
    IPC::barrier(JOINED, kNProcesses - 1);
    std::map<std::string, int> peers;
    ++peers[TestChordIndex::instance().predecessor_->id.ipPort()];
    ++peers[TestChordIndex::instance().successor_->id.ipPort()];
    IPC::barrier(SHARED_PEERS, kNProcesses - 1);
    for (size_t i = 0; i < 2 * kNProcesses; ++i) {
      std::string peer;
      IPC::pop(&peer);
      ++peers[peer];
    }
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

TEST_F(ChordIndexTestInitialized, localData) {
  EXPECT_TRUE(TestChordIndex::instance().addData("key", "data"));
  std::string result;
  EXPECT_TRUE(TestChordIndex::instance().retrieveData("key", &result));
  EXPECT_EQ("data", result);
}

TEST_F(ChordIndexTestInitialized, joinStabilizeAddRetrieve) {
  const size_t kNProcesses = FLAGS_chord_processes;
  const size_t kNData = 5;
  enum Barriers{INIT, ROOT_SHARED, JOINED_STABILIZED, ADDED_RETRIEVED};
  if (getSubprocessId() == 0) {
    std::ostringstream command_extra;
    command_extra << "--chord_processes=" << FLAGS_chord_processes;
    for (size_t i = 1; i < kNProcesses; ++i) {
      launchSubprocess(i, command_extra.str());
    }
    IPC::barrier(INIT, kNProcesses - 1);
    IPC::push(PeerId::self().ipPort());
    IPC::barrier(ROOT_SHARED, kNProcesses - 1);
    usleep(10 * kNProcesses * FLAGS_stabilize_us); // yes, 10 is a magic number
    // it should be an upper bound of the amount of required stabilization
    // iterations per process
    IPC::barrier(JOINED_STABILIZED, kNProcesses - 1);
    EXPECT_GT(kNProcesses, 1);
    for (size_t i = 0; i < kNData; ++i) {
      std::string key, value, result;
      addNonLocalData(&key, &value, i);
      EXPECT_TRUE(TestChordIndex::instance().retrieveData(key, &result));
      EXPECT_EQ(value, result);
    }
    IPC::barrier(ADDED_RETRIEVED, kNProcesses - 1);
  } else {
    IPC::barrier(INIT, kNProcesses - 1);
    IPC::barrier(ROOT_SHARED, kNProcesses - 1);
    std::string root_string;
    IPC::pop(&root_string);
    PeerId root(root_string);
    TestChordIndex::instance().join(root);
    IPC::barrier(JOINED_STABILIZED, kNProcesses - 1);
    IPC::barrier(ADDED_RETRIEVED, kNProcesses - 1);
  }
}

TEST_F(ChordIndexTestInitialized, joinStabilizeAddjoinStabilizeRetrieve) {
  const size_t kNProcessesHalf = FLAGS_chord_processes / 2;
  const size_t kNData = 10;
  enum Barriers{INIT, ROOT_SHARED, JOINED_STABILIZED__INIT_2,
    ADDED__ROOT_SHARED_2, JOINED_STABILIZED_2, RETRIEVED};
  if (getSubprocessId() == 0) {
    std::ostringstream command_extra;
    command_extra << "--chord_processes=" << FLAGS_chord_processes;
    // first round of joins
    for (size_t i = 1; i <= kNProcessesHalf; ++i) {
      launchSubprocess(i, command_extra.str());
    }
    IPC::barrier(INIT, kNProcessesHalf);
    IPC::push(PeerId::self().ipPort());
    IPC::barrier(ROOT_SHARED, kNProcessesHalf);
    // second round of joins
    for (size_t i = 1; i <= kNProcessesHalf; ++i) {
      launchSubprocess(kNProcessesHalf + i, command_extra.str());
    }
    // wait for stabilization of round 1
    usleep(10 * kNProcessesHalf * FLAGS_stabilize_us); // yes, 10 is a magic number
    // it should be an upper bound of the amount of required stabilization
    // iterations per process
    IPC::barrier(JOINED_STABILIZED__INIT_2, 2*kNProcessesHalf);
    EXPECT_GT(kNProcessesHalf, 0);
    TestChordIndex::DataMap data;
    for (size_t i = 0; i < kNData; ++i) {
      std::string key, value;
      addNonLocalData(&key, &value, i);
      EXPECT_TRUE(data.insert(std::make_pair(key, value)).second);
    }
    IPC::push(PeerId::self().ipPort());
    IPC::barrier(ADDED__ROOT_SHARED_2, 2*kNProcessesHalf);
    // wait for stabilization of round 2
    usleep(10 * kNProcessesHalf * FLAGS_stabilize_us);
    IPC::barrier(JOINED_STABILIZED_2, 2*kNProcessesHalf);
    for (const TestChordIndex::DataMap::value_type& item : data) {
      std::string result;
      EXPECT_TRUE(TestChordIndex::instance().retrieveData(item.first, &result));
      EXPECT_EQ(item.second, result);
    }
    IPC::barrier(RETRIEVED, 2*kNProcessesHalf);
  } else {
    PeerId root;
    std::string root_string;
    if (getSubprocessId() <= kNProcessesHalf){
      IPC::barrier(INIT, kNProcessesHalf);
      IPC::barrier(ROOT_SHARED, kNProcessesHalf);
      IPC::pop(&root_string);
      root = PeerId(root_string);
      TestChordIndex::instance().join(root);
    }
    IPC::barrier(JOINED_STABILIZED__INIT_2, 2*kNProcessesHalf);
    IPC::barrier(ADDED__ROOT_SHARED_2, 2*kNProcessesHalf);
    if (getSubprocessId() > kNProcessesHalf) {
      IPC::pop(&root_string);
      root = PeerId(root_string);
      TestChordIndex::instance().join(root);
    }
    IPC::barrier(JOINED_STABILIZED_2, 2*kNProcessesHalf);
    IPC::barrier(RETRIEVED, 2*kNProcessesHalf);
  }
}

} // namespace map_api

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
