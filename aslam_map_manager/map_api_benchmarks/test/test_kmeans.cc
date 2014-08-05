#include <random>

#include <gtest/gtest.h>

#include <multiagent_mapping_common/test/testing_entrypoint.h>
#include <map_api_test_suite/multiprocess_fixture.h>

#include "map_api_benchmarks/app.h"
#include "map_api_benchmarks/common.h"
#include "map_api_benchmarks/distance.h"
#include "map_api_benchmarks/kmeans-view.h"
#include "map_api_benchmarks/multi-kmeans-hoarder.h"
#include "map_api_benchmarks/multi-kmeans-worker.h"
#include "map_api_benchmarks/simple-kmeans.h"
#include "floating-point-test-helpers.h"

namespace map_api {
namespace benchmarks {

TEST(KmeansView, InsertFetch) {
  app::init();
  std::mt19937 generator(42);
  DescriptorVector descriptors_in, centers_in, descriptors_out, centers_out;
  std::vector<unsigned int> membership_in, membership_out;
  GenerateTestData(3, 3, generator(), 20., .5, &centers_in, &descriptors_in,
                   &membership_in);
  EXPECT_EQ(9, descriptors_in.size());
  EXPECT_EQ(3, centers_in.size());
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
  EXPECT_LT(descriptor_sum.norm(), 1e-5);

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

  // surprisingly works
  EXPECT_EQ(cluster_sum_product_in, cluster_sum_product_out);

  app::kill();
}

class MapApiBenchmarks : public map_api_test_suite::MultiprocessTest {
 protected:
  void SetUpImpl() {
    std::mt19937 generator(40);
    GenerateTestData(kNumfeaturesPerCluster, kNumClusters, generator(),
                     kAreaWidth, kClusterRadius,
                     &gt_centers_, &descriptors_, &gt_membership_);

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
  std::vector<unsigned int> gt_membership_, membership_;
};

TEST_F(MapApiBenchmarks, MultiKmeans) {
  MultiKmeansHoarder hoarder;
  map_api::Id data_chunk_id, center_chunk_id, membership_chunk_id;
  hoarder.init(descriptors_, gt_centers_, gt_membership_, kAreaWidth,
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
