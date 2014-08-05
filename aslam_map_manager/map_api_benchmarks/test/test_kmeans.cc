#include <random>

#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>
#include <map_api_test_suite/multiprocess_fixture.h>

#include "map_api_benchmarks/common.h"
#include "map_api_benchmarks/distance.h"
#include "map_api_benchmarks/multi-kmeans-hoarder.h"
#include "map_api_benchmarks/multi-kmeans-worker.h"
#include "map_api_benchmarks/simple-kmeans.h"
#include "floating-point-test-helpers.h"

namespace map_api {
namespace benchmarks {

class MapApiBenchmarks : public map_api_test_suite::MultiprocessTest {
 protected:
  void SetUpImpl() {
    std::mt19937 generator(40);
    GenerateTestData(kNumfeaturesPerCluster, kNumClusters, generator(),
                     kAreaWidth, kClusterRadius,
                     &gt_centers_, &descriptors_, &membership_,
                     &gt_membership_);

    ASSERT_FALSE(descriptors_.empty());
    ASSERT_EQ(descriptors_[0].size(), 2u);
  }

  void TearDownImpl() {
  }

  static constexpr size_t kNumfeaturesPerCluster = 20;
  static constexpr size_t kNumClusters = 20;
  static constexpr double kAreaWidth = 20.;
  static constexpr double kClusterRadius = .5;
  DescriptorVector gt_centers_;
  DescriptorVector descriptors_;
  std::vector<unsigned int> membership_;
  std::vector<unsigned int> gt_membership_;
};

TEST_F(MapApiBenchmarks, MultiKmeans) {
  MultiKmeansHoarder hoarder;
  map_api::Id data_chunk_id, center_chunk_id, membership_chunk_id;
  hoarder.init(descriptors_, gt_centers_, membership_, kAreaWidth,
               &data_chunk_id, &center_chunk_id, &membership_chunk_id);
}

TEST_F(MapApiBenchmarks, DISABLED_Kmeans) {
  std::mt19937 generator(40);
  // Init with ground-truth.
  std::shared_ptr<DescriptorVector> centers =
      aligned_shared<DescriptorVector>(gt_centers_);

  DescriptorType descriptor_zero;
  descriptor_zero.setConstant(kDescriptorDimensionality, 1,
                              static_cast<Scalar>(0));

  map_api::benchmarks::SimpleKmeans<DescriptorType,
  map_api::benchmarks::distance::L2<DescriptorType>,
  Eigen::aligned_allocator<DescriptorType> > kmeans(descriptor_zero);

  kmeans.SetInitMethod(
      map_api::benchmarks::InitGiven<DescriptorType>(descriptor_zero));

  kmeans.Cluster(descriptors_, kNumClusters, generator(), &membership_, &centers);

  std::vector<unsigned int> membercnt;
  membercnt.resize(centers->size(), 0);
  for (size_t i = 0; i < membership_.size(); ++i) {
    unsigned int member = membership_[i];
    EXPECT_EQ(membership_[i], gt_membership_[i]);
    ++membercnt[member];
  }
  for (size_t i = 0; i < membercnt.size(); ++i) {
    EXPECT_NE(membercnt[i], static_cast<unsigned int>(0));
  }

  map_api::benchmarks::distance::L2<DescriptorType> l2_distance;

  for (size_t descriptor_idx = 0; descriptor_idx < descriptors_.size();
      ++descriptor_idx) {
    DescriptorType& descriptor = descriptors_.at(descriptor_idx);
    int closest_center = -1;
    unsigned int closest_distance = std::numeric_limits<unsigned int>::max();
    unsigned int second_closest_distance =
        std::numeric_limits<unsigned int>::max();
    for (size_t center_idx = 0; center_idx < centers->size(); ++center_idx) {
      unsigned int distance = l2_distance(descriptor, centers->at(center_idx));
      if (distance < closest_distance) {
        second_closest_distance = closest_distance;
        closest_distance = distance;
        closest_center = center_idx;
      }
    }
    // Check that we don't have a trivial solution.
    EXPECT_NE(second_closest_distance, closest_distance);
    EXPECT_NE(closest_center, -1);
  }
}

}  // namespace map_api
}  // namespace benchmarks

MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
