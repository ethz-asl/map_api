#ifndef MAP_API_BENCHMARKS_KMEANS_SUBDIVISION_HOARDER_H_
#define MAP_API_BENCHMARKS_KMEANS_SUBDIVISION_HOARDER_H_

#include <vector>

#include "map_api_benchmarks/common.h"

namespace map_api {
namespace benchmarks {

class KmeansSubdivisionHoarder {
 public:
  KmeansSubdivisionHoarder(size_t degree, double max_dimension,
                           size_t num_centers);
  void init(const DescriptorVector& descriptors,
            const DescriptorVector& gt_centers, const Scalar area_width,
            int random_seed, Chunk** data_chunk, Chunks* center_chunks,
            Chunks* membership_chunks);
  void refresh();

  void refreshThread();
  void startRefreshThread();
  void stopRefreshThread();

 private:
  void plot(const DescriptorVector& descriptors,
            const DescriptorVector& centers,
            const std::vector<unsigned int>& membership);

  FILE* gnuplot_;
  Chunk* descriptor_chunk_;
  Chunks center_chunks_, membership_chunks_;
  std::thread refresh_thread_;
  volatile bool terminate_refresh_thread_;

  size_t degree_;
  double max_dimension_;
  size_t num_centers_;
};

} /* namespace benchmarks */
} /* namespace map_api */

#endif  // MAP_API_BENCHMARKS_KMEANS_SUBDIVISION_HOARDER_H_
