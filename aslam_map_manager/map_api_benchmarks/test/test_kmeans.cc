#include <random>

#include <Eigen/Core>
#include <multiagent_mapping_common/aligned_allocation.h>
#include <multiagent_mapping_common/test/testing_entrypoint.h>
#include <map_api_test_suite/multiprocess_fixture.h>

#include "map_api_benchmarks/distance.h"
#include "map_api_benchmarks/simple_kmeans.h"

class MapApiBenchmarks : public map_api_test_suite::MultiprocessTest {
 protected:
  void SetUpImpl() {
    constexpr int kSeed = 42;
    constexpr int kNumSamples = 20;
    constexpr double kMinValue = 0;
    constexpr double kMaxValue = 10;
    GenerateRandomData(kSeed, kNumSamples, kMinValue, kMaxValue);
  }

  void TearDownImpl() {

  }

  void GenerateRandomData(int seed, int num_samples, double min, double max) {
    data_.resize(num_samples);

    std::mt19937 gen(seed);
    std::uniform_real_distribution<> dis(min, max);

    for (unsigned int i = 0; i < data_.size(); ++i) {
      Eigen::Vector2d& sample = data_[i];
      for (int j = 0; j < sample.rows(); ++j) {
        sample(j) = dis(gen);
      }
    }
  }

  Aligned<std::vector, Eigen::Vector2d>::type data_;
};


TEST_F(MapApiBenchmarks, Kmeans) {
  typedef Eigen::Vector2d Feature;
  typedef map_api_benchmarks::distance::L2<Feature> Distance;
  typedef Eigen::aligned_allocator<Feature> Allocator;
  map_api_benchmarks::SimpleKmeans<Feature, Distance, Allocator> kmeans;

  std::vector<unsigned int> membership;
  std::shared_ptr<Aligned<std::vector, Eigen::Vector2d>::type> centers(
      new Aligned<std::vector, Eigen::Vector2d>::type);

  kmeans.Cluster(data_, 2, 42, &membership, &centers);

  EXPECT_EQ(membership.size(), data_.size());
  EXPECT_EQ(centers->size(), 2);
}

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
