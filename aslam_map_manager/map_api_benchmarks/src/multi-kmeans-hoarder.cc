#include "map_api_benchmarks/multi-kmeans-hoarder.h"

#include <glog/logging.h>

#include "map_api_benchmarks/app.h"
#include "map_api_benchmarks/kmeans-view.h"

namespace map_api {
namespace benchmarks {

DEFINE_bool(gnuplot_persist, false, "if set, gnuplot detaches from test");

void MultiKmeansHoarder::init(
    const DescriptorVector& descriptors, const DescriptorVector& gt_centers,
    const Scalar area_width, map_api::Id* data_chunk_id,
    map_api::Id* center_chunk_id, map_api::Id* membership_chunk_id) {
  CHECK_NOTNULL(data_chunk_id);
  CHECK_NOTNULL(center_chunk_id);
  CHECK_NOTNULL(membership_chunk_id);

  // generate random centers and membership
  std::shared_ptr<DescriptorVector> centers =
      aligned_shared<DescriptorVector>(gt_centers);
  std::vector<unsigned int> membership;
  DescriptorType descriptor_zero;
  descriptor_zero.setConstant(kDescriptorDimensionality, 1,
                              static_cast<Scalar>(0));
  Kmeans2D generator(descriptor_zero);
  generator.SetMaxIterations(0);
  // TODO(tcies) rng
  generator.Cluster(descriptors, centers->size(), 3, &membership, &centers);

  // load into database
  descriptor_chunk_ = app::data_point_table->newChunk();
  center_chunk_ = app::center_table->newChunk();
  membership_chunk_ = app::association_table->newChunk();
  *data_chunk_id = descriptor_chunk_->id();
  *center_chunk_id = center_chunk_->id();
  *membership_chunk_id = membership_chunk_->id();
  KmeansView exporter(descriptor_chunk_, center_chunk_, membership_chunk_);
  exporter.insert(descriptors, *centers, membership);

  // launch gnuplot
  if (FLAGS_gnuplot_persist) {
    gnuplot_ = popen("gnuplot --persist", "w");
  } else {
    gnuplot_ = popen("gnuplot", "w");
  }
  fprintf(gnuplot_, "set xrange [0:%f]\n", area_width);
  fprintf(gnuplot_, "set yrange [0:%f]\n", area_width);
  fputs("set size square\nset key off\n", gnuplot_);

  plot(descriptors, *centers);
}

void MultiKmeansHoarder::refresh() {
  DescriptorVector descriptors, centers;
  std::vector<unsigned int> membership;
  KmeansView view(descriptor_chunk_, center_chunk_, membership_chunk_);
  view.fetch(&descriptors, &centers, &membership);
  plot(descriptors, centers);
}

void MultiKmeansHoarder::plot(const DescriptorVector& descriptors,
                              const DescriptorVector& centers) {
  fputs("plot '-' w p, '-' w p lt rgb \"blue\"\n", gnuplot_);
  for (const DescriptorType descriptor : descriptors) {
    fprintf(gnuplot_, "%f %f\n", descriptor[0], descriptor[1]);
  }
  fputs("e\n", gnuplot_);
  for (const DescriptorType center : centers) {
    fprintf(gnuplot_, "%f %f\n", center[0], center[1]);
  }
  fputs("e\n", gnuplot_);
  fflush(gnuplot_);
}

} /* namespace benchmarks */
} /* namespace map_api */
