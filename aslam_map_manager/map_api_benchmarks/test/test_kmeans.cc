#include <random>

#include <Eigen/Core>
#include <multiagent_mapping_common/aligned_allocation.h>
#include <multiagent_mapping_common/test/testing_entrypoint.h>
#include <map_api_test_suite/multiprocess_fixture.h>

#include "map_api_benchmarks/distance.h"
#include "map_api_benchmarks/simple_kmeans.h"
#include "floating-point-test-helpers.h"

class MapApiBenchmarks : public map_api_test_suite::MultiprocessTest {
 protected:
  void SetUpImpl() {

    std::mt19937 generator(40);
    GenerateTestData(kNumfeaturesPerCluster, kNumClusters, generator(),
                     &gt_centers, &descriptors, &membership,
                     &gt_membership);
  }

  void TearDownImpl() {

  }

  static constexpr size_t kNumfeaturesPerCluster = 100;
  static constexpr size_t kNumClusters = 100;
  DescriptorVector gt_centers;
  DescriptorVector descriptors;
  std::vector<unsigned int> membership;
  std::vector<unsigned int> gt_membership;

};


TEST_F(MapApiBenchmarks, Kmeans) {
  std::mt19937 generator(40);
  // Init with ground-truth.
  std::shared_ptr<DescriptorVector> centers =
      aligned_shared<DescriptorVector>(gt_centers);

  DescriptorType descriptor_zero;
  descriptor_zero.setConstant(kDescriptorDimensionality, 1,
                              static_cast<Scalar>(0));

  map_api::benchmarks::SimpleKmeans<DescriptorType,
      map_api::benchmarks::distance::L2<DescriptorType>,
      Eigen::aligned_allocator<DescriptorType> > kmeans(descriptor_zero);

  kmeans.SetInitMethod(
      map_api::benchmarks::InitGiven<DescriptorType>(descriptor_zero));

  kmeans.Cluster(descriptors, kNumClusters, generator(), &membership, &centers);

  std::vector<unsigned int> membercnt;
  membercnt.resize(centers->size(), 0);
  for (size_t i = 0; i < membership.size(); ++i) {
    unsigned int member = membership[i];
    EXPECT_EQ(membership[i], gt_membership[i]);
    ++membercnt[member];
  }
  for (size_t i = 0; i < membercnt.size(); ++i) {
    EXPECT_NE(membercnt[i], static_cast<unsigned int>(0));
  }

  map_api::benchmarks::distance::L2<DescriptorType> l2_distance;

  for (size_t descriptor_idx = 0; descriptor_idx < descriptors.size();
      ++descriptor_idx) {
    DescriptorType& descriptor = descriptors.at(descriptor_idx);
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


MULTIAGENT_MAPPING_UNITTEST_ENTRYPOINT
