#ifndef MAP_API_BENCHMARKS_KMEANS_SUBDIVISION_WORKER_H_
#define MAP_API_BENCHMARKS_KMEANS_SUBDIVISION_WORKER_H_

#include <vector>

#include "map_api_benchmarks/common.h"
#include "map_api_benchmarks/kmeans-subdivision-view.h"

namespace map_api {
namespace benchmarks {

class KmeansSubdivisionWorker {
 public:
  /**
   * Parameter copy intended
   */
  KmeansSubdivisionWorker(size_t degree, double max_dimension,
                          size_t num_centers, Chunk* descriptor_chunk,
                          Chunks center_chunks, Chunks membership_chunks);

  bool clusterOnceOne(size_t target_cluster, int random_seed,
                      size_t ms_before_commit);

 private:
  DistanceType::result_type clusterOnce(
      int random_seed, std::shared_ptr<DescriptorVector>* centers,
      std::vector<unsigned int>* membership, KmeansSubdivisionView* view);

  size_t degree_;
  double max_dimension_;
  size_t num_centers_;
  Chunk* descriptor_chunk_;
  Chunks center_chunks_, membership_chunks_;
};

} /* namespace benchmarks */
} /* namespace map_api */

#endif  // MAP_API_BENCHMARKS_KMEANS_SUBDIVISION_WORKER_H_
