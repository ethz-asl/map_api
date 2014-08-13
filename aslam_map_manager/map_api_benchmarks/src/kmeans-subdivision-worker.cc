#include "map_api_benchmarks/kmeans-subdivision-worker.h"

#include <multiagent_mapping_common/conversions.h>

namespace map_api {
namespace benchmarks {

KmeansSubdivisionWorker::KmeansSubdivisionWorker(
    size_t degree, double max_dimension, size_t num_centers,
    Chunk* descriptor_chunk, Chunks center_chunks, Chunks membership_chunks)
    : degree_(degree),
      max_dimension_(max_dimension),
      num_centers_(num_centers),
      descriptor_chunk_(CHECK_NOTNULL(descriptor_chunk)),
      center_chunks_(center_chunks),
      membership_chunks_(membership_chunks) {}

bool KmeansSubdivisionWorker::clusterOnceOne(size_t target_cluster,
                                             int random_seed,
                                             size_t ms_before_commit) {
  DistanceType::result_type result;
  DescriptorVector descriptors;
  std::shared_ptr<DescriptorVector> centers(new DescriptorVector);
  std::vector<unsigned int> membership;
  KmeansSubdivisionView view(degree_, max_dimension_, num_centers_,
                             center_chunks_, membership_chunks_,
                             descriptor_chunk_);
  result = clusterOnce(random_seed, &centers, &membership, &view);
  usleep(ms_before_commit * kMillisecondsToMicroseconds);
  return view.updateCenterRelated(target_cluster, *centers, membership);
}

DistanceType::result_type KmeansSubdivisionWorker::clusterOnce(
    int random_seed, std::shared_ptr<DescriptorVector>* centers,
    std::vector<unsigned int>* membership, KmeansSubdivisionView* view) {
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
