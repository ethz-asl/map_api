#ifndef MAP_API_BENCHMARKS_COMMON_H_
#define MAP_API_BENCHMARKS_COMMON_H_

#include <string>
#include <vector>

#include <Eigen/Core>
#include <glog/logging.h>

#include <map-api/chunk.h>
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

typedef map_api::benchmarks::distance::L2<DescriptorType> DistanceType;

typedef map_api::benchmarks::SimpleKmeans<DescriptorType, DistanceType,
    Eigen::aligned_allocator<DescriptorType> > Kmeans2D;

typedef std::vector<Chunk*> Chunks;

inline Id numToId(size_t num) {
  Id result;
  char suffix[4];
  memset(suffix, 0, sizeof(suffix));
  snprintf(suffix, sizeof(suffix), "%03lu", num);
  CHECK(result.fromHexString("00000000000000000000000000000" +
                             std::string(suffix)));
  return result;
}

}  // namespace benchmarks
}  // namespace map_api

UNIQUE_ID_DEFINE_ID_HASH(map_api::benchmarks::AssociationId);
UNIQUE_ID_DEFINE_ID_HASH(map_api::benchmarks::DataPointId);
UNIQUE_ID_DEFINE_ID_HASH(map_api::benchmarks::CenterId);

#endif  // MAP_API_BENCHMARKS_COMMON_H_
