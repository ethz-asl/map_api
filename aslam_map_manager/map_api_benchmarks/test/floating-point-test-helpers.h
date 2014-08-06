// Copyright 2013 Motorola Mobility LLC. Part of the Trailmix project.
// CONFIDENTIAL. AUTHORIZED USE ONLY. DO NOT REDISTRIBUTE.
#ifndef MAP_API_BENCHMARKS_FLOATING_POINT_TEST_HELPERS_H_
#define MAP_API_BENCHMARKS_FLOATING_POINT_TEST_HELPERS_H_
#include <cstdio>
#include <random>
#include <vector>

namespace map_api {
namespace benchmarks {

void GenerateTestData(size_t kNumfeaturesPerCluster, size_t kNumClusters,
                      size_t seed, double area_width, double cluster_radius,
                      DescriptorVector* const gt_centers,
                      DescriptorVector* const descriptors,
                      std::vector<unsigned int>* const gt_membership) {
  CHECK_NOTNULL(gt_centers);
  CHECK_NOTNULL(descriptors);
  CHECK_NOTNULL(gt_membership);
  CHECK_GT(area_width, 2 * cluster_radius);
  std::mt19937 generator(seed);

  std::uniform_real_distribution<double> center_generator(
      cluster_radius, area_width - cluster_radius);
  std::uniform_real_distribution<double> noise_generator(
      0., area_width);
  std::normal_distribution<double> data_generator(
      0, cluster_radius);
  std::default_random_engine engine;

  for (size_t i = 0; i < 100; ++i) {
    DescriptorType sample = DescriptorType::Zero(kDescriptorDimensionality, 1);
    for (int k = 0; k < kDescriptorDimensionality; ++k) {
      sample(k, 0) += noise_generator(engine);
    }
    descriptors->push_back(sample);
  }

  for (size_t i = 0; i < kNumClusters; ++i) {
    DescriptorType center;
    center.resize(kDescriptorDimensionality, Eigen::NoChange);
    for (int j = 0; j < kDescriptorDimensionality; ++j) {
      center(j, 0) = center_generator(engine);
    }
    gt_centers->push_back(center);

    for (size_t j = 0; j < kNumfeaturesPerCluster; ++j) {
      DescriptorType sample = center;
      for (int k = 0; k < kDescriptorDimensionality; ++k) {
        sample(k, 0) += data_generator(engine);
      }
      descriptors->push_back(sample);
      gt_membership->push_back(i);
    }
  }
}

}  // namespace map_api
}  // namespace benchmarks

#endif  // MAP_API_BENCHMARKS_FLOATING_POINT_TEST_HELPERS_H_
