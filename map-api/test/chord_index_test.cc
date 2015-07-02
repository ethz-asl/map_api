#include <fstream>  // NOLINT
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest_prod.h>
#include <timing/timer.h>

#include "map-api/core.h"
#include "map-api/ipc.h"
#include "map-api/test/testing-entrypoint.h"
#include "./map_api_fixture.h"
#include "./test_chord_index.cc"

DEFINE_uint64(wait_for_stabilize_us, 1000,
              "Interval of stabilization in microseconds");

namespace map_api {

const std::string kRetrieveDataTimerTag = "retrieveData";
const std::string kRetrieveDataTimeFile = "retrieve_time.txt";

class ChordIndexTest : public MapApiFixture {
 protected:
  virtual void SetUp() override {
    // not using MultiprocessFixture::SetUp intentionally - need to register
    // handlers first
    TestChordIndex::staticInit();
    Core::initializeInstance();
    CHECK_NOTNULL(Core::instance());
  }
};

TEST_F(ChordIndexTest, create) {
  TestChordIndex::instance().create();
  EXPECT_TRUE(TestChordIndex::instance().initialized_);
  TestChordIndex::instance().leave();
  EXPECT_FALSE(TestChordIndex::instance().initialized_);
}

DEFINE_uint64(addresses_to_hash, 5, "Amount of addresses to hash");
TEST_F(ChordIndexTest, hashDistribution) {
  enum Barriers {
    INIT,
    HASH,
    DIE
  };
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
      std::string hash_string = IPC::pop<std::string>();
      hash_ss << ", " << hash_string;
    }
    hash_ss << "]";
    LOG(INFO) << hash_ss.str();
    IPC::barrier(DIE, FLAGS_addresses_to_hash - 1);
  } else {
    IPC::barrier(INIT, FLAGS_addresses_to_hash - 1);
    std::ostringstream hash_ss;
    hash_ss << ChordIndex::hash(PeerId::self());
    IPC::push(hash_ss.str());
    IPC::barrier(HASH, FLAGS_addresses_to_hash - 1);
    IPC::barrier(DIE, FLAGS_addresses_to_hash - 1);
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
    for (size_t salt = 0;; ++salt) {
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
  enum Barriers {
    INIT,
    ROOT_SHARED,
    JOINED,
    SHARED_PEERS
  };
  if (getSubprocessId() == 0) {
    std::ostringstream command_extra;
    command_extra << "--chord_processes=" << FLAGS_chord_processes;
    for (size_t i = 1; i < kNProcesses; ++i) {
      launchSubprocess(i, command_extra.str());
    }
    IPC::barrier(INIT, kNProcesses - 1);
    IPC::push(PeerId::self());
    IPC::barrier(ROOT_SHARED, kNProcesses - 1);
    usleep(10 * kNProcesses *
           FLAGS_wait_for_stabilize_us);  // yes, 10 is a magic number
    // it should be an upper bound of the amount of required stabilization
    // iterations per process
    IPC::barrier(JOINED, kNProcesses - 1);
    std::map<std::string, int> peers;
    ++peers[TestChordIndex::instance().predecessor_->id.ipPort()];
    ++peers[TestChordIndex::instance().successor_->id.ipPort()];
    IPC::barrier(SHARED_PEERS, kNProcesses - 1);
    for (size_t i = 0; i < 2 * (kNProcesses - 1); ++i) {
      std::string peer = IPC::pop<std::string>();
      ++peers[peer];
    }
    for (const std::pair<std::string, int> peer_count : peers) {
      EXPECT_EQ(2, peer_count.second);
    }
  } else {
    IPC::barrier(INIT, kNProcesses - 1);
    IPC::barrier(ROOT_SHARED, kNProcesses - 1);
    PeerId root = IPC::pop<PeerId>();
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
  const size_t kNData = 100;
  enum Barriers {
    INIT,
    ROOT_SHARED,
    JOINED_STABILIZED,
    ADDED_RETRIEVED
  };
  if (getSubprocessId() == 0) {
    for (size_t i = 1; i < kNProcesses; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT, kNProcesses - 1);
    IPC::push(PeerId::self());
    IPC::barrier(ROOT_SHARED, kNProcesses - 1);
    usleep(10 * kNProcesses *
           FLAGS_wait_for_stabilize_us);  // yes, 10 is a magic number
    // it should be an upper bound of the amount of required stabilization
    // iterations per process
    IPC::barrier(JOINED_STABILIZED, kNProcesses - 1);
    EXPECT_GT(kNProcesses, 1u);
    for (size_t i = 0; i < kNData; ++i) {
      std::string key, value, result;
      addNonLocalData(&key, &value, i);
      timing::Timer timer(kRetrieveDataTimerTag);
      EXPECT_TRUE(TestChordIndex::instance().retrieveData(key, &result));
      timer.Stop();
      EXPECT_EQ(value, result);
    }
    std::ofstream file(kRetrieveDataTimeFile, std::ios::out);
    file << timing::Timing::GetMeanSeconds(kRetrieveDataTimerTag) << " "
         << timing::Timing::GetMinSeconds(kRetrieveDataTimerTag) << " "
         << timing::Timing::GetMaxSeconds(kRetrieveDataTimerTag) << std::endl;
    LOG(INFO) << timing::Timing::Print();
    IPC::barrier(ADDED_RETRIEVED, kNProcesses - 1);
  } else {
    IPC::barrier(INIT, kNProcesses - 1);
    IPC::barrier(ROOT_SHARED, kNProcesses - 1);
    PeerId root = IPC::pop<PeerId>();
    TestChordIndex::instance().join(root);
    IPC::barrier(JOINED_STABILIZED, kNProcesses - 1);
    IPC::barrier(ADDED_RETRIEVED, kNProcesses - 1);
  }
}

TEST_F(ChordIndexTestInitialized, joinStabilizeAddjoinStabilizeRetrieve) {
  const size_t kNProcessesHalf = FLAGS_chord_processes / 2;
  const size_t kNData = 10;
  enum Barriers {
    INIT,
    ROOT_SHARED,
    JOINED_STABILIZED__INIT_2,
    ADDED__ROOT_SHARED_2,
    JOINED_STABILIZED_2,
    RETRIEVED
  };
  if (getSubprocessId() == 0) {
    // first round of joins
    for (size_t i = 1; i <= kNProcessesHalf; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT, kNProcessesHalf);
    IPC::push(PeerId::self());
    IPC::barrier(ROOT_SHARED, kNProcessesHalf);
    // second round of joins
    for (size_t i = 1; i <= kNProcessesHalf; ++i) {
      launchSubprocess(kNProcessesHalf + i);
    }
    // wait for stabilization of round 1
    usleep(10 * kNProcessesHalf * FLAGS_wait_for_stabilize_us);
    // yes, 10 is a magic number
    // it should be an upper bound of the amount of required stabilization
    // iterations per process
    IPC::barrier(JOINED_STABILIZED__INIT_2, 2 * kNProcessesHalf);
    EXPECT_GT(kNProcessesHalf, 0u);
    TestChordIndex::DataMap data;
    for (size_t i = 0; i < kNData; ++i) {
      std::string key, value;
      addNonLocalData(&key, &value, i);
      EXPECT_TRUE(data.insert(std::make_pair(key, value)).second);
    }
    IPC::push(PeerId::self());
    IPC::barrier(ADDED__ROOT_SHARED_2, 2 * kNProcessesHalf);
    // wait for stabilization of round 2
    usleep(10 * kNProcessesHalf * FLAGS_wait_for_stabilize_us);
    IPC::barrier(JOINED_STABILIZED_2, 2 * kNProcessesHalf);
    for (const TestChordIndex::DataMap::value_type& item : data) {
      std::string result;
      EXPECT_TRUE(TestChordIndex::instance().retrieveData(item.first, &result));
      EXPECT_EQ(item.second, result);
    }
    IPC::barrier(RETRIEVED, 2 * kNProcessesHalf);
  } else {
    PeerId root;
    if (getSubprocessId() <= kNProcessesHalf) {
      IPC::barrier(INIT, kNProcessesHalf);
      IPC::barrier(ROOT_SHARED, kNProcessesHalf);
      root = IPC::pop<PeerId>();
      TestChordIndex::instance().join(root);
    }
    IPC::barrier(JOINED_STABILIZED__INIT_2, 2 * kNProcessesHalf);
    IPC::barrier(ADDED__ROOT_SHARED_2, 2 * kNProcessesHalf);
    if (getSubprocessId() > kNProcessesHalf) {
      root = IPC::pop<PeerId>();
      TestChordIndex::instance().join(root);
    }
    IPC::barrier(JOINED_STABILIZED_2, 2 * kNProcessesHalf);
    IPC::barrier(RETRIEVED, 2 * kNProcessesHalf);
  }
}

TEST_F(ChordIndexTestInitialized, joinStabilizeAddLeaveStabilizeRetrieve) {
  const size_t kNProcessesHalf = FLAGS_chord_processes / 2;
  const size_t kNData = 10;
  enum Barriers {
    INIT,
    ROOT_SHARED,
    JOINED_STABILIZED,
    ADDED,
    LEFT,
    RETRIEVED
  };
  if (getSubprocessId() == 0) {
    for (size_t i = 1; i <= 2 * kNProcessesHalf; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT, 2 * kNProcessesHalf);
    IPC::push(PeerId::self());
    IPC::barrier(ROOT_SHARED, 2 * kNProcessesHalf);
    usleep(20 * kNProcessesHalf * FLAGS_wait_for_stabilize_us);
    IPC::barrier(JOINED_STABILIZED, 2 * kNProcessesHalf);
    EXPECT_GT(kNProcessesHalf, 0u);
    TestChordIndex::DataMap data;
    for (size_t i = 0; i < kNData; ++i) {
      std::string key, value;
      addNonLocalData(&key, &value, i);
      EXPECT_TRUE(data.insert(std::make_pair(key, value)).second);
    }
    IPC::barrier(ADDED, 2 * kNProcessesHalf);
    usleep(10 * kNProcessesHalf * FLAGS_wait_for_stabilize_us);
    IPC::barrier(LEFT, kNProcessesHalf);
    for (const TestChordIndex::DataMap::value_type& item : data) {
      std::string result;
      EXPECT_TRUE(TestChordIndex::instance().retrieveData(item.first, &result));
      EXPECT_EQ(item.second, result);
    }
    IPC::barrier(RETRIEVED, kNProcessesHalf);

  } else {
    IPC::barrier(INIT, 2 * kNProcessesHalf);
    IPC::barrier(ROOT_SHARED, 2 * kNProcessesHalf);
    PeerId root = IPC::pop<PeerId>();
    TestChordIndex::instance().join(root);
    IPC::barrier(JOINED_STABILIZED, 2 * kNProcessesHalf);
    IPC::barrier(ADDED, 2 * kNProcessesHalf);
    if (getSubprocessId() <= kNProcessesHalf) {
      IPC::barrier(LEFT, kNProcessesHalf);
      IPC::barrier(RETRIEVED, kNProcessesHalf);
    }
  }
}

TEST_F(ChordIndexTestInitialized,
       joinStabilizeAddjoinStabilizeUpdateLeaveRetrieve) {
  const size_t kNProcessesHalf = FLAGS_chord_processes / 2;
  const size_t kNData = 10;
  enum Barriers {
    INIT,
    ROOT_SHARED,
    JOINED_STABILIZED__INIT_2,
    ADDED__ROOT_SHARED_2,
    JOINED_STABILIZED_2,
    UPDATED,
    LEFT,
    RETRIEVED
  };
  if (getSubprocessId() == 0) {
    // first round of joins
    for (size_t i = 1; i <= kNProcessesHalf; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT, kNProcessesHalf);
    IPC::push(PeerId::self());
    IPC::barrier(ROOT_SHARED, kNProcessesHalf);
    // second round of joins
    for (size_t i = 1; i <= kNProcessesHalf; ++i) {
      launchSubprocess(kNProcessesHalf + i);
    }
    // wait for stabilization of round 1
    usleep(10 * kNProcessesHalf * FLAGS_wait_for_stabilize_us);
    // yes, 10 is a magic number
    // it should be an upper bound of the amount of required stabilization
    // iterations per process
    IPC::barrier(JOINED_STABILIZED__INIT_2, 2 * kNProcessesHalf);
    EXPECT_GT(kNProcessesHalf, 0u);
    TestChordIndex::DataMap data;
    for (size_t i = 0; i < kNData; ++i) {
      std::string key, value;
      addNonLocalData(&key, &value, i);
      EXPECT_TRUE(data.insert(std::make_pair(key, value)).second);
    }
    IPC::push(PeerId::self());
    IPC::barrier(ADDED__ROOT_SHARED_2, 2 * kNProcessesHalf);
    // wait for stabilization of round 2
    usleep(10 * kNProcessesHalf * FLAGS_wait_for_stabilize_us);
    IPC::barrier(JOINED_STABILIZED_2, 2 * kNProcessesHalf);
    size_t i = 0;
    for (TestChordIndex::DataMap::value_type& item : data) {
      std::ostringstream new_value;
      new_value << "updated" << i;
      item.second = new_value.str();
      EXPECT_TRUE(
          TestChordIndex::instance().addData(item.first, new_value.str()));
      if (++i == kNProcessesHalf) {
        break;
      }
    }
    IPC::barrier(UPDATED, kNProcessesHalf);
    usleep(10 * kNProcessesHalf * FLAGS_wait_for_stabilize_us);
    IPC::barrier(LEFT, kNProcessesHalf);
    for (const TestChordIndex::DataMap::value_type& item : data) {
      std::string result;
      EXPECT_TRUE(TestChordIndex::instance().retrieveData(item.first, &result));
      EXPECT_EQ(item.second, result);
    }
    IPC::barrier(RETRIEVED, kNProcessesHalf);
  } else {
    PeerId root;
    if (getSubprocessId() <= kNProcessesHalf) {
      IPC::barrier(INIT, kNProcessesHalf);
      IPC::barrier(ROOT_SHARED, kNProcessesHalf);
      root = IPC::pop<PeerId>();
      TestChordIndex::instance().join(root);
    }
    IPC::barrier(JOINED_STABILIZED__INIT_2, 2 * kNProcessesHalf);
    IPC::barrier(ADDED__ROOT_SHARED_2, 2 * kNProcessesHalf);
    if (getSubprocessId() > kNProcessesHalf) {
      root = IPC::pop<PeerId>();
      TestChordIndex::instance().join(root);
    }
    IPC::barrier(JOINED_STABILIZED_2, 2 * kNProcessesHalf);
    IPC::barrier(UPDATED, 2 * kNProcessesHalf);
    if (getSubprocessId() <= kNProcessesHalf) {
      IPC::barrier(LEFT, kNProcessesHalf);
      IPC::barrier(RETRIEVED, kNProcessesHalf);
    }
  }
}

TEST_F(ChordIndexTestInitialized, fingerRetrieveLength) {
  const size_t kNProcesses = FLAGS_chord_processes;
  enum Barriers {
    INIT,
    ROOT_SHARED,
    JOINED_STABILIZED,
    FINGERS_READY,
    GET_PREDECESSOR
  };
  if (getSubprocessId() == 0) {
    for (size_t i = 1; i < kNProcesses; ++i) {
      launchSubprocess(i);
    }
    IPC::barrier(INIT, kNProcesses - 1);
    IPC::push(PeerId::self());
    IPC::barrier(ROOT_SHARED, kNProcesses - 1);
    usleep(20 * kNProcesses *
           FLAGS_wait_for_stabilize_us);  // yes, 10 is a magic number
    // it should be an upper bound of the amount of required stabilization
    // iterations per process
    IPC::barrier(JOINED_STABILIZED, kNProcesses - 1);
    while (!TestChordIndex::instance().areFingersReady()) {
      usleep(2000);
    }
    IPC::barrier(FINGERS_READY, kNProcesses - 1);
    PeerId predecessor;
    ChordIndex::Key key = ChordIndex::hash(PeerId::self()) - 1;
    size_t count =
        TestChordIndex::instance().findPredecessorCountRpcs(key, &predecessor);
    EXPECT_LT(count, kNProcesses);
    IPC::barrier(GET_PREDECESSOR, kNProcesses - 1);
  } else {
    IPC::barrier(INIT, kNProcesses - 1);
    IPC::barrier(ROOT_SHARED, kNProcesses - 1);
    PeerId root = IPC::pop<PeerId>();
    TestChordIndex::instance().join(root);
    IPC::barrier(JOINED_STABILIZED, kNProcesses - 1);
    while (!TestChordIndex::instance().areFingersReady()) {
      usleep(2000);
    }
    IPC::barrier(FINGERS_READY, kNProcesses - 1);
    IPC::barrier(GET_PREDECESSOR, kNProcesses - 1);
  }
}

}  // namespace map_api

MAP_API_UNITTEST_ENTRYPOINT
