#ifndef MAP_API_BENCHMARKS_MULTI_KMEANS_WORKER_H_
#define MAP_API_BENCHMARKS_MULTI_KMEANS_WORKER_H_

#include <map-api/chunk.h>

#include "map_api_benchmarks/common.h"
#include "map_api_benchmarks/kmeans-view.h"

namespace map_api {
namespace benchmarks {

class MultiKmeansWorker {
 public:
  MultiKmeansWorker(Chunk* descriptor_chunk, Chunk* center_chunk,
                    Chunk* membership_chunk);

  DistanceType::result_type clusterOnceAll(int random_seed);

  bool clusterOnceOne(size_t target_cluster, int random_seed);

 private:
  DistanceType::result_type clusterOnce(
      int random_seed, std::shared_ptr<DescriptorVector>* centers,
      std::vector<unsigned int>* membership, KmeansView* view);

  Chunk* descriptor_chunk_, *center_chunk_, *membership_chunk_;
};

} /* namespace benchmarks */
} /* namespace map_api */

#endif /* MAP_API_BENCHMARKS_MULTI_KMEANS_WORKER_H_ */
