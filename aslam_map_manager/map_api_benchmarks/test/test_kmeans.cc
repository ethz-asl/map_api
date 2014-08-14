#include <fstream>  // NOLINT
#include <random>

#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>
#include <map_api_test_suite/multiprocess_fixture.h>
#include <map-api/ipc.h>
#include <timing/timer.h>
#include <statistics/statistics.h>

#include "map_api_benchmarks/app.h"
#include "map_api_benchmarks/common.h"
#include "map_api_benchmarks/distance.h"
#include "map_api_benchmarks/kmeans-subdivision-hoarder.h"
#include "map_api_benchmarks/kmeans-subdivision-worker.h"
#include "map_api_benchmarks/kmeans-view.h"
#include "map_api_benchmarks/multi-kmeans-hoarder.h"
#include "map_api_benchmarks/multi-kmeans-worker.h"
#include "map_api_benchmarks/simple-kmeans.h"
#include "./floating-point-test-helpers.h"

namespace map_api {
namespace benchmarks {

TEST(KmeansView, InsertFetch) {
  constexpr size_t kClusters = 100, kDataPerCluster = 100;
  constexpr Scalar kTolerance = 1e-4 * kClusters * kDataPerCluster;
  app::init();
  std::mt19937 generator(42);
  DescriptorVector descriptors_in, centers_in, descriptors_out, centers_out;
  std::vector<unsigned int> membership_in, membership_out;
  GenerateTestData(kClusters, kDataPerCluster, 0, generator(), 20., .5,
                   &centers_in, &descriptors_in, &membership_in);
  EXPECT_EQ(kClusters * kDataPerCluster, descriptors_in.size());
  EXPECT_EQ(kClusters, centers_in.size());
  EXPECT_EQ(descriptors_in.size(), membership_in.size());

  Chunk* descriptor_chunk = app::data_point_table->newChunk();
  Chunk* center_chunk = app::center_table->newChunk();
  Chunk* membership_chunk = app::association_table->newChunk();
  KmeansView exporter(descriptor_chunk, center_chunk, membership_chunk);
  exporter.insert(descriptors_in, centers_in, membership_in);

  KmeansView importer(descriptor_chunk, center_chunk, membership_chunk);
  importer.fetch(&descriptors_out, &centers_out, &membership_out);
  EXPECT_EQ(descriptors_in.size(), descriptors_out.size());
  EXPECT_EQ(centers_in.size(), centers_out.size());

  // total checksum
  DescriptorType descriptor_sum;
  descriptor_sum.resize(2, Eigen::NoChange);
  descriptor_sum.setZero();
  for (const DescriptorType& in_descriptor : descriptors_in) {
    descriptor_sum += in_descriptor;
  }
  for (const DescriptorType& out_descriptor : descriptors_out) {
    descriptor_sum -= out_descriptor;
  }
  EXPECT_LT(descriptor_sum.norm(), kTolerance);

  // per-cluster checksum (data could be re-arranged)
  Scalar cluster_sum_product_in = 1., cluster_sum_product_out = 1.;
  for (size_t i = 0; i < centers_in.size(); ++i) {
    DescriptorType cluster_sum;
    cluster_sum.resize(2, Eigen::NoChange);
    cluster_sum.setZero();
    for (size_t j = 0; j < descriptors_in.size(); ++j) {
      if (membership_in[j] == i) {
        cluster_sum += descriptors_in[j];
      }
    }
    cluster_sum_product_in *= cluster_sum.norm();
  }
  for (size_t i = 0; i < centers_out.size(); ++i) {
    DescriptorType cluster_sum;
    cluster_sum.resize(2, Eigen::NoChange);
    cluster_sum.setZero();
    for (size_t j = 0; j < descriptors_out.size(); ++j) {
      if (membership_out[j] == i) {
        cluster_sum += descriptors_out[j];
      }
    }
    cluster_sum_product_out *= cluster_sum.norm();
  }
  EXPECT_EQ(cluster_sum_product_in, cluster_sum_product_out);

  app::kill();
}

DEFINE_uint64(num_clusters, 10, "Amount of clusters in kmeans experiment");

class MultiKmeans : public map_api_test_suite::MultiprocessTest {
 protected:
  void SetUpImpl() {
    app::init();
    if (getSubprocessId() == 0) {
      DescriptorVector gt_centers;
      DescriptorVector descriptors;
      std::vector<unsigned int> gt_membership, membership;
      generator_ = std::mt19937(time(NULL));
      GenerateTestData(kNumfeaturesPerCluster, kNumClusters, kNumNoise,
                       generator_(), kAreaWidth, kClusterRadius,
                       &gt_centers, &descriptors, &gt_membership);
      ASSERT_FALSE(descriptors.empty());
      ASSERT_EQ(descriptors[0].size(), 2u);
      hoarder_.init(descriptors, gt_centers, kAreaWidth, generator_(),
                    &data_chunk_, &center_chunk_, &membership_chunk_);
    }
  }

  void TearDownImpl() {
    app::kill();
  }

  void popIdsInitWorker() {
    map_api::Id data_chunk_id, center_chunk_id, membership_chunk_id;
    CHECK(IPC::pop(&data_chunk_id));
    CHECK(IPC::pop(&center_chunk_id));
    CHECK(IPC::pop(&membership_chunk_id));
    data_chunk_ = app::data_point_table->getChunk(data_chunk_id);
    center_chunk_ = app::center_table->getChunk(center_chunk_id);
    membership_chunk_ = app::association_table->getChunk(membership_chunk_id);
    worker_.reset(
        new MultiKmeansWorker(data_chunk_, center_chunk_, membership_chunk_));
  }

  void pushIds() const {
    IPC::push(data_chunk_->id());
    IPC::push(center_chunk_->id());
    IPC::push(membership_chunk_->id());
  }

  void initChunkLogging() { membership_chunk_->enableLockLogging(); }

  static void clearFile(const char* file_name) {
    std::ofstream filestream;
    filestream.open(file_name, std::ios::out | std::ios::trunc);
    filestream.close();
  }

  static void putRankMeanMinMax(const char* file_name, const char* tag) {
    std::ofstream filestream;
    filestream.open(file_name, std::ios::out | std::ios::app);
    filestream << PeerId::selfRank() << " " <<
        timing::Timing::GetMeanSeconds(tag) << " " <<
        timing::Timing::GetMinSeconds(tag) << " " <<
        timing::Timing::GetMaxSeconds(tag) << std::endl;
    filestream.close();
  }

  template <typename ValueType>
  static void putValue(const char* file_name, const ValueType& value) {
    std::ofstream filestream;
    filestream.open(file_name, std::ios::out | std::ios::app);
    filestream << value << std::endl;
    filestream.close();
  }

  template <typename ValueTypeA, typename ValueTypeB>
  static void putValues(const char* file_name, const ValueTypeA& value_a,
                        const ValueTypeB& value_b) {
    std::ofstream filestream;
    filestream.open(file_name, std::ios::out | std::ios::app);
    filestream << value_a << " " << value_b << std::endl;
    filestream.close();
  }

  size_t kNumClusters = FLAGS_num_clusters;
  static constexpr size_t kNumfeaturesPerCluster = 40;
  static constexpr size_t kNumNoise = 100;
  static constexpr double kAreaWidth = 20.;
  static constexpr double kClusterRadius = 1;

  Chunk* data_chunk_, *center_chunk_, *membership_chunk_;
  MultiKmeansHoarder hoarder_;
  std::unique_ptr<MultiKmeansWorker> worker_;
  std::mt19937 generator_;

  const char* kAcceptanceFile = "meas_acceptance.txt";
  const char* kSequenceFile = "meas_sequence.txt";
  const char* kReadLockFile = "meas_readlock.txt";
  const char* kWriteLockFile = "meas_writelock.txt";

  const char* kAcceptanceTag = "acceptance-rate";
  const char* kReadLockTag = "map_api::Chunk::distributedReadLock";
  const char* kWriteLockTag = "map_api::Chunk::distributedWriteLock";
};

TEST_F(MultiKmeans, KmeansHoarderWorker) {
  enum Processes {HOARDER, WORKER};
  constexpr size_t kIterations = 10;
  int current_barrier = 0;
  DistanceType::result_type result;
  std::vector<DistanceType::result_type> results;
  if (getSubprocessId() == HOARDER) {
    launchSubprocess(WORKER);
    IPC::barrier(current_barrier++, 1);
    pushIds();
    IPC::barrier(current_barrier++, 1);
    // wait for worker to collect chunks and optimize once
    for (size_t i = 0; i < kIterations; ++i) {
      IPC::barrier(current_barrier++, 1);
      std::string result_string;
      IPC::pop(&result_string);
      std::istringstream ss(result_string);
      ss >> result;
      results.push_back(result);
      hoarder_.refresh();
    }
    CHECK_EQ(kIterations, results.size());
    for (size_t i = 1; i < kIterations; ++i) {
      EXPECT_LE(results[i], results[i-1]);
    }
  }
  if (getSubprocessId() == WORKER) {
    IPC::barrier(current_barrier++, 1);
    // wait for hoarder to send chunk ids
    IPC::barrier(current_barrier++, 1);
    popIdsInitWorker();
    for (size_t i = 0; i < kIterations; ++i) {
      result = worker_->clusterOnceAll(generator_());
      std::ostringstream ss;
      ss << result;
      IPC::push(ss.str());
      IPC::barrier(current_barrier++, 1);
    }
  }
}

DEFINE_uint64(process_time, 0, "Simulated time between fetch and commit");
DEFINE_uint64(num_iterations, 5, "Amount of iterations in multi-kmeans");

TEST_F(MultiKmeans, CenterWorkers) {
  enum Barriers {
    INIT,
    IDS_PUSHED,
    LOG_SYNC,
    DIE
  };
  const size_t kIterations = FLAGS_num_iterations;
  statistics::StatsCollector accept_reject(kAcceptanceTag);
  if (getSubprocessId() == 0) {
    for (size_t i = 1; i <= kNumClusters; ++i) {
      launchSubprocess(i);
    }
    clearFile(kAcceptanceFile);
    clearFile(kSequenceFile);
    clearFile(kReadLockFile);
    clearFile(kWriteLockFile);
    IPC::barrier(INIT, kNumClusters);
    pushIds();
    IPC::barrier(IDS_PUSHED, kNumClusters);
    IPC::barrier(LOG_SYNC, kNumClusters);  // only approximate sync
    initChunkLogging();
    // TODO(tcies) trigger!
    hoarder_.startRefreshThread();
    IPC::barrier(DIE, kNumClusters);
    hoarder_.stopRefreshThread();
  } else {
    IPC::barrier(INIT, kNumClusters);
    // wait for hoarder to send chunk ids
    IPC::barrier(IDS_PUSHED, kNumClusters);
    popIdsInitWorker();
    IPC::barrier(LOG_SYNC, kNumClusters);  // only approximate sync
    initChunkLogging();
    for (size_t i = 0; i < kIterations; ++i) {
      if (worker_->clusterOnceOne(getSubprocessId() - 1, generator_(),
                                  FLAGS_process_time)) {
        accept_reject.AddSample(1);
      } else {
        accept_reject.AddSample(0);
      }
      putValue(kSequenceFile, PeerId::selfRank());
    }
    LOG(INFO) << timing::Timing::Print();
    LOG(INFO) << statistics::Statistics::Print();
    putValues(kAcceptanceFile, PeerId::selfRank(),
              statistics::Statistics::GetMean(kAcceptanceTag));
    putRankMeanMinMax(kReadLockFile, kReadLockTag);
    putRankMeanMinMax(kWriteLockFile, kWriteLockTag);
    IPC::barrier(DIE, kNumClusters);
  }
}

DEFINE_uint64(subdivision_degree, 2, "Amount of sectors per dimension");

class SubdividedKmeans : public map_api_test_suite::MultiprocessTest {
 protected:
  void SetUpImpl() {
    app::init();
    if (getSubprocessId() == 0) {
      DescriptorVector gt_centers;
      DescriptorVector descriptors;
      std::vector<unsigned int> gt_membership, membership;
      generator_ = std::mt19937(time(NULL));
      GenerateTestData(kNumfeaturesPerCluster, kNumClusters, kNumNoise,
                       generator_(), kAreaWidth, kClusterRadius, &gt_centers,
                       &descriptors, &gt_membership);
      ASSERT_FALSE(descriptors.empty());
      ASSERT_EQ(descriptors[0].size(), 2u);
      hoarder_.reset(
          new KmeansSubdivisionHoarder(kDegree, kAreaWidth, kNumClusters));
      hoarder_->init(descriptors, gt_centers, kAreaWidth, generator_(),
                     &data_chunk_, &center_chunks_, &membership_chunks_);
    }
  }

  void TearDownImpl() { app::kill(); }

  void workerInit() {
    data_chunk_ = app::data_point_table->getChunk(numToId(0u));
    for (size_t i = 0u; i < kNumClusters; ++i) {
      center_chunks_.push_back(app::center_table->getChunk(numToId(i)));
    }
    for (size_t i = 0u; i < kDegree * kDegree; ++i) {
      membership_chunks_.push_back(
          app::association_table->getChunk(numToId(i)));
    }
    worker_.reset(new KmeansSubdivisionWorker(kDegree, kAreaWidth, kNumClusters,
                                              data_chunk_, center_chunks_,
                                              membership_chunks_));
  }

  static void clearFile(const char* file_name) {
    std::ofstream filestream;
    filestream.open(file_name, std::ios::out | std::ios::trunc);
    filestream.close();
  }

  template <typename ValueTypeA, typename ValueTypeB>
  static void putRankValues(const char* file_name, const ValueTypeA& value_a,
                            const ValueTypeB& value_b) {
    std::ofstream filestream;
    filestream.open(file_name, std::ios::out | std::ios::app);
    filestream << PeerId::selfRank() << " " << value_a << " " << value_b
               << std::endl;
    filestream.close();
  }

  size_t kNumClusters = FLAGS_num_clusters;
  static constexpr size_t kNumfeaturesPerCluster = 40;
  static constexpr size_t kNumNoise = 100;
  static constexpr double kAreaWidth = 20.;
  static constexpr double kClusterRadius = 1;
  size_t kDegree = FLAGS_subdivision_degree;

  Chunk* data_chunk_;
  Chunks center_chunks_, membership_chunks_;
  std::unique_ptr<KmeansSubdivisionHoarder> hoarder_;
  std::unique_ptr<KmeansSubdivisionWorker> worker_;
  std::mt19937 generator_;

  const char* kLockCountFile = "meas_lock_count.txt";
  const char* kLockDurationFile = "meas_lock_duration.txt";

  std::string kLockCountTag =
      "map_api::NetTableTransaction::lock - " + app::kAssociationTableName;
  const char* kLockDurationTag = "map_api::Transaction::commit - lock";
};

TEST_F(SubdividedKmeans, CenterWorkers) {
  enum Barriers {
    INIT,
    IDS_PUSHED,
    LOG_SYNC,
    DIE
  };
  const size_t kIterations = FLAGS_num_iterations;
  if (getSubprocessId() == 0) {
    for (size_t i = 1; i <= kNumClusters; ++i) {
      launchSubprocess(i);
    }
    clearFile(kLockCountFile);
    clearFile(kLockDurationFile);
    IPC::barrier(INIT, kNumClusters);
    IPC::barrier(LOG_SYNC, kNumClusters);  // only approximate sync
    // TODO(tcies) trigger!
    hoarder_->startRefreshThread();
    IPC::barrier(DIE, kNumClusters);
    hoarder_->stopRefreshThread();
  } else {
    IPC::barrier(INIT, kNumClusters);
    workerInit();
    IPC::barrier(LOG_SYNC, kNumClusters);  // only approximate sync
    for (size_t i = 0; i < kIterations; ++i) {
      if (worker_->clusterOnceOne(getSubprocessId() - 1, generator_(),
                                  FLAGS_process_time)) {
      } else {
      }
    }
    putRankValues(kLockCountFile,
                  statistics::Statistics::GetMean(kLockCountTag),
                  sqrt(statistics::Statistics::GetVariance(kLockCountTag)));
    putRankValues(kLockDurationFile,
                  timing::Timing::GetMeanSeconds(kLockDurationTag),
                  sqrt(timing::Timing::GetVarianceSeconds(kLockDurationTag)));
    LOG(INFO) << "Done";
    IPC::barrier(DIE, kNumClusters);
  }
}

}  // namespace benchmarks
}  // namespace map_api

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
