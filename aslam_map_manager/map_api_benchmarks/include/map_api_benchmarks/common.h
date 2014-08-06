#ifndef MAP_API_BENCHMARKS_COMMON_H_
#define MAP_API_BENCHMARKS_COMMON_H_

#include <Eigen/Core>
#include <glog/logging.h>

#include <multiagent_mapping_common/aligned_allocation.h>
#include <multiagent_mapping_common/unique_id.h>

#include "map_api_benchmarks/distance.h"
#include "map_api_benchmarks/simple-kmeans.h"

namespace map_api {
namespace benchmarks {
UNIQUE_ID_DEFINE_ID(AssociationId);
UNIQUE_ID_DEFINE_ID(DataPointId);
UNIQUE_ID_DEFINE_ID(CenterId);

typedef float Scalar;
typedef Eigen::Matrix<Scalar, Eigen::Dynamic, 1> DescriptorType;
constexpr int kDescriptorDimensionality = 2;
typedef Aligned<std::vector, DescriptorType>::type DescriptorVector;

typedef map_api::benchmarks::SimpleKmeans<DescriptorType,
    map_api::benchmarks::distance::L2<DescriptorType>,
    Eigen::aligned_allocator<DescriptorType> > Kmeans2D;

}  // namespace map_api
}  // namespace benchmarks

UNIQUE_ID_DEFINE_ID_HASH(map_api::benchmarks::AssociationId);
UNIQUE_ID_DEFINE_ID_HASH(map_api::benchmarks::DataPointId);
UNIQUE_ID_DEFINE_ID_HASH(map_api::benchmarks::CenterId);

#endif /* MAP_API_BENCHMARKS_COMMON_H_ */
