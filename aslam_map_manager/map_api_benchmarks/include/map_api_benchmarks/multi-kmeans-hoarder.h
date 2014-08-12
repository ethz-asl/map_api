#ifndef MAP_API_BENCHMARKS_MULTI_KMEANS_HOARDER_H_
#define MAP_API_BENCHMARKS_MULTI_KMEANS_HOARDER_H_

#include <vector>

#include <map-api/chunk.h>
#include <map-api/id.h>

#include "map_api_benchmarks/common.h"

namespace map_api {
namespace benchmarks {

class MultiKmeansHoarder {
 public:
  /**
   * Commits the supplied data as initial state to the Map API, and displays
   * it with gnuplot
   */
  void init(const DescriptorVector& descriptors,
            const DescriptorVector& gt_centers, const Scalar area_width,
            int random_seed, Chunk** data_chunk, Chunk** center_chunk,
            Chunk** membership_chunk);
  /**
   * Refreshes the latest Map API data to gnuplot
   */
  void refresh();

  void refreshThread();
  void startRefreshThread();
  void stopRefreshThread();

 private:
  void plot(const DescriptorVector& descriptors,
            const DescriptorVector& centers,
            const std::vector<unsigned int>& membership);

  FILE* gnuplot_;
  Chunk* descriptor_chunk_, *center_chunk_, *membership_chunk_;
  std::thread refresh_thread_;
  volatile bool terminate_refresh_thread_;
};

} /* namespace benchmarks */
} /* namespace map_api */

#endif /* MAP_API_BENCHMARKS_MULTI_KMEANS_HOARDER_H_ */
