#ifndef MAP_API_BENCHMARKS_MULTI_KMEANS_WORKER_H_
#define MAP_API_BENCHMARKS_MULTI_KMEANS_WORKER_H_

#include <map-api/chunk.h>

#include "map_api_benchmarks/common.h"

namespace map_api {
namespace benchmarks {

class MultiKmeansWorker {
 public:
  MultiKmeansWorker(Chunk* descriptor_chunk, Chunk* center_chunk,
                    Chunk* membership_chunk);

  DistanceType::result_type clusterOnceAll(int random_seed);

  void clusterOnceOne();

 private:
  Chunk* descriptor_chunk_, *center_chunk_, *membership_chunk_;
};

} /* namespace benchmarks */
} /* namespace map_api */

#endif /* MAP_API_BENCHMARKS_MULTI_KMEANS_WORKER_H_ */
