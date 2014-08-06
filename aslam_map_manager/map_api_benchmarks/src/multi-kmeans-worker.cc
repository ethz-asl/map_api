#include "map_api_benchmarks/multi-kmeans-worker.h"

#include "map_api_benchmarks/common.h"
#include "map_api_benchmarks/kmeans-view.h"
#include "map_api_benchmarks/simple-kmeans.h"

namespace map_api {
namespace benchmarks {

MultiKmeansWorker::MultiKmeansWorker(Chunk* descriptor_chunk,
                                     Chunk* center_chunk,
                                     Chunk* membership_chunk)
: descriptor_chunk_(descriptor_chunk), center_chunk_(center_chunk),
  membership_chunk_(membership_chunk) {}

DistanceType::result_type MultiKmeansWorker::clusterOnceAll(
    int random_seed)  {
  DistanceType::result_type result;
  KmeansView view(descriptor_chunk_, center_chunk_, membership_chunk_);
  DescriptorVector descriptors;
  std::shared_ptr<DescriptorVector> centers(new DescriptorVector);
  std::vector<unsigned int> membership;
  view.fetch(&descriptors, centers.get(), &membership);

  DescriptorType descriptor_zero;
  descriptor_zero.setConstant(kDescriptorDimensionality, 1,
                              static_cast<Scalar>(0));
  Kmeans2D clusterer(descriptor_zero);
  clusterer.SetMaxIterations(1);
  clusterer.SetInitMethod(InitGiven<DescriptorType>(descriptor_zero));
  // TODO(seed)
  result = clusterer.Cluster(descriptors, centers->size(), random_seed,
                             &membership, &centers);
  view.updateAll(*centers, membership);
  return result;
}

} /* namespace benchmarks */
} /* namespace map_api */
