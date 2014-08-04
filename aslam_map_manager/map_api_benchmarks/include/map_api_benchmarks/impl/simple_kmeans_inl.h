// Copyright 2013 Motorola Mobility LLC. Part of the Trailmix project.
// CONFIDENTIAL. AUTHORIZED USE ONLY. DO NOT REDISTRIBUTE.
// Original code Copyright Willowgarage as part of ROS, adapted within Trailmix.
// http://ros.org/wiki/vocabulary_tree
#ifndef MAP_API_BENCHMARKS_SIMPLE_KMEANS_INL_H_
#define MAP_API_BENCHMARKS_SIMPLE_KMEANS_INL_H_

#include <algorithm>
#include <iostream>  // NOLINT
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/logging.h>

namespace map_api_benchmarks {

// Eigen Matrix type features.
template <class Feature, class FeatureAllocator>
typename std::enable_if<std::is_floating_point<typename Feature::Scalar>::value,
                        void>::type
ClusterMean(const std::vector<Feature*, FeatureAllocator>& features,
            Feature* const mean) {
  CHECK_NOTNULL(mean);
  mean->setZero();
  for (const Feature* const& feature : features) {  // NOLINT
    *mean += *feature;
  }
  *mean /= features.size();
}

// Binary features.
template <class Feature, class FeatureAllocator>
typename std::enable_if<std::is_integral<typename Feature::value_type>::value,
                        void>::type
ClusterMean(const std::vector<Feature*, FeatureAllocator>& features,
            Feature* const mean) {
  CHECK_NOTNULL(mean);
  DescriptorMean(features, mean);
}

template <class Feature, class Distance, class FeatureAllocator>
SimpleKmeans<Feature, Distance, FeatureAllocator>::SimpleKmeans(
    const Feature& zero, const Distance& d)
    : zero_(zero),
      distance_(d),
      choose_centers_(InitKMeansPlusPlus<Feature>(zero)),
      max_iterations_(100),
      restarts_(1) {}

template <class Feature, class Distance, class FeatureAllocator>
typename SimpleKmeans<Feature, Distance, FeatureAllocator>::SquaredDistanceType
SimpleKmeans<Feature, Distance, FeatureAllocator>::Cluster(
    const std::vector<Feature, FeatureAllocator>& features, size_t k,
    int random_seed, std::vector<unsigned int>* const membership,
    typename SimpleKmeans<Feature, Distance,
                          FeatureAllocator>::Centers* const centers) const {
  CHECK_NOTNULL(centers);
  CHECK_NOTNULL(membership);
  std::vector<Feature*> feature_ptrs;
  feature_ptrs.reserve(features.size());
  for (const Feature& f : features) {
    feature_ptrs.push_back(const_cast<Feature*>(&f));
  }
  membership->resize(feature_ptrs.size(), -1);
  return ClusterPointers(feature_ptrs, k, random_seed, centers, membership);
}

template <class Feature, class Distance, class FeatureAllocator>
typename SimpleKmeans<Feature, Distance, FeatureAllocator>::SquaredDistanceType
SimpleKmeans<Feature, Distance, FeatureAllocator>::ClusterPointers(
    const std::vector<Feature*>& features, size_t k, int random_seed,
    typename SimpleKmeans<Feature, Distance,
                          FeatureAllocator>::Centers* const centers,
    std::vector<unsigned int>* const membership) const {
  CHECK_NOTNULL(centers);
  CHECK(*centers);
  CHECK_NOTNULL(membership);
  std::mt19937 generator(random_seed);
  typedef typename SimpleKmeans<Feature, Distance, FeatureAllocator>::Centers
      Centers;

  Centers new_centers =
      aligned_shared<std::vector<Feature, FeatureAllocator> >(**centers);

  std::vector<unsigned int> new_membership(features.size());
  (*centers)->clear();

  SquaredDistanceType least_sse =
      std::numeric_limits<SquaredDistanceType>::max();

  for (size_t starts = 0; starts < restarts_; ++starts) {
    choose_centers_(features, k, distance_, generator(), new_centers.get());

    VLOG(3) << "#\tCluster run " << starts;
    VLOG(3) << "Have " << new_centers->size() << " centers" << std::endl;
    SquaredDistanceType sse =
        ClusterOnce(features, k, generator(), &new_centers, &new_membership);
    if (sse < least_sse) {
      least_sse = sse;
      *centers = new_centers;
      *membership = new_membership;
    }
  }
  CHECK(!(*centers)->empty());
  return least_sse;
}

// This class is the default implementation of a search accelerator that
// computes the distance of the feature to all centers. It therefore does
// not speed up anything.
template <class Feature, class Distance, class FeatureAllocator>
class DefaultSearchAccelerator {
  typedef typename SimpleKmeans<Feature, Distance,
                                FeatureAllocator>::SquaredDistanceType
      SquaredDistanceType;
  typedef std::pair<typename Distance::result_type, int> Candidate;

 public:
  DefaultSearchAccelerator(
      const std::shared_ptr<std::vector<Feature, FeatureAllocator> >&
          database_features,
      const Distance& distance)
      : database_features_(database_features), distance_(distance) {
    CHECK(database_features);
  }
  void GetNNearestNeighbors(
      const Feature& query_feature, int num_neighbors,
      std::vector<int>* neighbors,
      std::vector<typename Distance::result_type>* distances) const {
    CHECK_NOTNULL(neighbors);
    CHECK_NOTNULL(distances);

    CHECK(database_features_);
    const std::vector<Feature, FeatureAllocator>& database =
        *database_features_;

    candidates_.clear();
    candidates_.reserve(database.size());

    for (unsigned int i = 0; i < database.size(); ++i) {
      SquaredDistanceType distance = distance_(query_feature, database[i]);
      candidates_.emplace_back(distance, i);
    }

      // Take the top n matches with smallest distance.
    std::sort(candidates_.begin(), candidates_.end(),
              [](const Candidate & lhs, const Candidate & rhs)->bool {
      return lhs.first < rhs.first;
    });
    distances->reserve(num_neighbors);
    neighbors->reserve(num_neighbors);
    for (size_t i = 0; i < num_neighbors && i < candidates_.size(); ++i) {
      distances->push_back(candidates_[i].first);
      neighbors->push_back(candidates_[i].second);
    }
  }

  unsigned int FindIndexOfClosestFeature(const Feature& query_feature) const {
    CHECK(database_features_);
    if (database_features_->empty()) {
      return 0;
    }
    unsigned int nearest = -1;
    SquaredDistanceType d_min = std::numeric_limits<SquaredDistanceType>::max();

    // Find the nearest cluster center to feature i.
    for (unsigned int i = 0; i < database_features_->size(); ++i) {
      SquaredDistanceType distance =
          distance_(query_feature, (*database_features_)[i]);
      if (distance < d_min) {
        d_min = distance;
        nearest = i;
      }
    }
    return nearest;
  }

 private:
  std::shared_ptr<std::vector<Feature, FeatureAllocator> > database_features_;
  std::vector<Candidate> candidates_;
  Distance distance_;
};

template <class Feature, class Distance, class FeatureAllocator,
          class SearchAccelerator>
struct ThreadedClusteringHelper {
  ThreadedClusteringHelper(
      const SearchAccelerator& search_accelerator,
      const std::vector<Feature*>* features,
      const std::vector<Feature, FeatureAllocator>* centers,
      std::vector<unsigned int>* membership)
      : search_accelerator_(search_accelerator),
        features_(features),
        centers_(centers),
        membership_(membership) {}
  void operator()(const std::vector<size_t>& range) const {
    for (size_t i : range) {
      // Find the nearest cluster center to feature i.
      unsigned int nearest =
          search_accelerator_.FindIndexOfClosestFeature(*(*features_)[i]);

      // Check that this feature is not assigned yet.
      CHECK_EQ((*membership_)[i], static_cast<unsigned int>(-1));
      (*membership_)[i] = nearest;
    }
  }
  const SearchAccelerator& search_accelerator_;
  const std::vector<Feature*>* features_;
  const std::vector<Feature, FeatureAllocator>* centers_;
  std::vector<unsigned int>* membership_;
};

template <class Feature, class Distance, class FeatureAllocator>
typename SimpleKmeans<Feature, Distance, FeatureAllocator>::SquaredDistanceType
SimpleKmeans<Feature, Distance, FeatureAllocator>::ClusterOnce(
    const std::vector<Feature*>& features, size_t k, int random_seed,
    typename SimpleKmeans<Feature, Distance,
                          FeatureAllocator>::Centers* const centers,
    std::vector<unsigned int>* const membership) const {
  CHECK_NOTNULL(centers);
  CHECK_NOTNULL(membership);

  std::mt19937 generator(random_seed);
  std::vector<size_t> new_center_counts(k);
  std::vector<std::vector<Feature*> > new_centers(k);

  for (size_t iter = 0; iter < max_iterations_; ++iter) {
    bool is_stable = true;

      // Reset Feature assignment.
    std::for_each(new_centers.begin(), new_centers.end(),
                  [](std::vector<Feature*> & i) {  // NOLINT
      i.clear();
    });

      // Reset membercount.
    std::for_each(new_center_counts.begin(), new_center_counts.end(),
                  [](size_t & i) {
      i = 0;
    });  // NOLINT

    if (features.size() > 1000) {
      VLOG(3) << "\t#" << iter << " Assigning descriptors" << std::endl;
    }

    // Prepare accelerator for finding the closest center:
    typedef DefaultSearchAccelerator<Feature, Distance, FeatureAllocator>
        SearchAccelerator;
    SearchAccelerator search_accelerator(*centers, distance_);

    std::vector<unsigned int> new_membership;
    new_membership.resize(features.size(), -1);

    constexpr int kNumBlocks = 16;
    {
      std::vector<std::vector<size_t> > blocks;

      if (features.size() < kNumBlocks * 2) {
        blocks.resize(1);
      } else {
        blocks.resize(kNumBlocks);
      }

      ThreadedClusteringHelper<Feature, Distance, FeatureAllocator,
                               SearchAccelerator>
          accelerator(search_accelerator, &features, centers->get(),
                      &new_membership);

      helpers::ParallelProcess<16>(features.size(), accelerator);
    }

    int num_unstable = 0;
    for (unsigned int i = 0; i < new_membership.size(); ++i) {
      // Check that this feature has been assigned.
      CHECK_NE(new_membership.at(i), static_cast<unsigned int>(-1));

      // Assign feature i to the cluster it is nearest to.
      if ((*membership)[i] != new_membership[i]) {
        is_stable = false;
        ++num_unstable;
        (*membership)[i] = new_membership[i];
      }
      // Accumulate the cluster center and its membership count.
      new_centers[new_membership[i]].push_back(features[i]);
      ++new_center_counts[new_membership[i]];
    }

    if (is_stable) {
      LOG(WARNING) << "\t#" << iter << " Stable!" << std::endl;
      break;
    }

    if (features.size() > 1000) {
      LOG(WARNING) << "\t#" << iter << " Recomputing centers. (Unstable: "
                   << num_unstable << ")" << std::endl;
    }

    auto mean_functor = [&new_center_counts,
                         &new_centers,
                         &centers ](const std::vector<size_t> & block)->void {
      for (size_t i : block) {
        if (new_center_counts[i] > 0) {
          ClusterMean(new_centers[i], &(**centers)[i]);
        }
      }
    };

    helpers::ParallelProcess<16>(new_centers.size(), mean_functor);

    // For singleton centers, choose new points sequentially.
    for (size_t i = 0; i < k; ++i) {
      if (new_center_counts[i] == 0) {
        // Choose a new center randomly from the input features
        bool found_unused = true;
        do {  // Make sure we don't choose a feature which is already a center.
          found_unused = true;
          unsigned int index = generator() % features.size();
          (**centers)[i] = *features[index];
          (*membership)[index] = i;
          for (size_t centeridx = 0; centeridx < k; ++centeridx) {
            if (centeridx == i) continue;
            if (distance_((**centers)[centeridx], (**centers)[i]) == 0) {
              found_unused = false;
              break;
            }
          }
        } while (!found_unused);
      }
    }
  }

  // Return the sum squared error
  SquaredDistanceType sse = SquaredDistanceType();
  for (size_t i = 0; i < features.size(); ++i) {
    sse += distance_(*features[i], (**centers)[ (*membership)[i]]);
  }
  return sse;
}
}       // namespace map_api_benchmarks
#endif  // MAP_API_BENCHMARKS_SIMPLE_KMEANS_INL_H_
