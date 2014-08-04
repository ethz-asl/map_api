// Copyright 2013 Motorola Mobility LLC. Part of the Trailmix project.
// CONFIDENTIAL. AUTHORIZED USE ONLY. DO NOT REDISTRIBUTE.
// Original code Copyright Willowgarage as part of ROS, adapted within Trailmix.
// http://ros.org/wiki/vocabulary_tree
#ifndef MAP_API_BENCHMARKS_FEATURE_ALLOCATOR_H_
#define MAP_API_BENCHMARKS_FEATURE_ALLOCATOR_H_

#include <memory>

#include <Eigen/StdVector>
#include <Eigen/Core>

namespace map_api_benchmarks {

/**
 * \brief Meta-function to get the default allocator for a particular feature type.
 *
 * Defaults to \c std::allocator<Feature>.
 */
template<class Feature>
struct DefaultAllocator {
  typedef std::allocator<Feature> type;
};

// Specialization to use aligned allocator for Eigen::Matrix types.
template<typename Scalar, int Rows, int Cols, int Options, int MaxRows,
    int MaxCols>
struct DefaultAllocator<
    Eigen::Matrix<Scalar, Rows, Cols, Options, MaxRows, MaxCols> > {
  typedef Eigen::aligned_allocator<
      Eigen::Matrix<Scalar, Rows, Cols, Options, MaxRows, MaxCols> > type;
};
}  //  namespace map_api_benchmarks
#endif  // MAP_API_BENCHMARKS_FEATURE_ALLOCATOR_H_
