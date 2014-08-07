#include "map_api_benchmarks/multi-kmeans-worker.h"

#include "map_api_benchmarks/common.h"
#include "map_api_benchmarks/simple-kmeans.h"

namespace map_api {
namespace benchmarks {

MultiKmeansWorker::MultiKmeansWorker(Chunk* descriptor_chunk,
                                     Chunk* center_chunk,
                                     Chunk* membership_chunk)
: descriptor_chunk_(CHECK_NOTNULL(descriptor_chunk)),
  center_chunk_(CHECK_NOTNULL(center_chunk)),
  membership_chunk_(CHECK_NOTNULL(membership_chunk)) {}

DistanceType::result_type MultiKmeansWorker::clusterOnceAll(
    int random_seed)  {
  DistanceType::result_type result;
  std::shared_ptr<DescriptorVector> centers;
  std::vector<unsigned int> membership;
  KmeansView view(descriptor_chunk_, center_chunk_, membership_chunk_);
  result = clusterOnce(random_seed, &centers, &membership, &view);
  view.updateAll(*centers, membership);
  return result;
}

bool MultiKmeansWorker::clusterOnceOne(size_t target_cluster, int random_seed) {
  DistanceType::result_type result;
  std::shared_ptr<DescriptorVector> centers;
  std::vector<unsigned int> membership;
  KmeansView view(descriptor_chunk_, center_chunk_, membership_chunk_);
  result = clusterOnce(random_seed, &centers, &membership, &view);
  return view.updateCenterRelated(target_cluster, *centers, membership);
}

DistanceType::result_type MultiKmeansWorker::clusterOnce(
    int random_seed, std::shared_ptr<DescriptorVector>* centers,
    std::vector<unsigned int>* membership, KmeansView* view) {
  CHECK_NOTNULL(centers);
  CHECK_NOTNULL(membership);
  CHECK_NOTNULL(view);
  DescriptorVector descriptors;
  centers->reset(new DescriptorVector);
  view->fetch(&descriptors, centers->get(), membership);

  DescriptorType descriptor_zero;
  descriptor_zero.setConstant(kDescriptorDimensionality, 1,
                              static_cast<Scalar>(0));
  Kmeans2D clusterer(descriptor_zero);
  clusterer.SetMaxIterations(1);
  clusterer.SetInitMethod(InitGiven<DescriptorType>(descriptor_zero));
  return clusterer.Cluster(descriptors, (*centers)->size(), random_seed,
                           membership, centers);
}

} /* namespace benchmarks */
} /* namespace map_api */
