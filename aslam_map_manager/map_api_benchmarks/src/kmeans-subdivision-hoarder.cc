#include "map_api_benchmarks/kmeans-subdivision-hoarder.h"

#include "map_api_benchmarks/app.h"
#include "map_api_benchmarks/kmeans-subdivision-view.h"

namespace map_api {
namespace benchmarks {

DECLARE_bool(gnuplot_persist);
DECLARE_bool(enable_visualization);

KmeansSubdivisionHoarder::KmeansSubdivisionHoarder(size_t degree,
                                                   double max_dimension,
                                                   size_t num_centers)
    : degree_(degree),
      max_dimension_(max_dimension),
      num_centers_(num_centers) {}

void KmeansSubdivisionHoarder::init(const DescriptorVector& descriptors,
                                    const DescriptorVector& gt_centers,
                                    const Scalar area_width, int random_seed,
                                    Chunk** data_chunk, Chunks* center_chunks,
                                    Chunks* membership_chunks) {
  CHECK_NOTNULL(data_chunk);
  CHECK_NOTNULL(center_chunks);
  CHECK_NOTNULL(membership_chunks);

  // generate random centers and membership
  std::shared_ptr<DescriptorVector> centers =
      aligned_shared<DescriptorVector>(gt_centers);
  std::vector<unsigned int> membership;
  DescriptorType descriptor_zero;
  descriptor_zero.setConstant(kDescriptorDimensionality, 1,
                              static_cast<Scalar>(0));
  Kmeans2D generator(descriptor_zero);
  generator.SetInitMethod(InitRandom<DescriptorType>(descriptor_zero));
  generator.SetMaxIterations(0);
  generator.Cluster(descriptors, centers->size(), random_seed, &membership,
                    &centers);

  // load into database
  descriptor_chunk_ = app::data_point_table->newChunk(numToId(0u));
  CHECK_NOTNULL(descriptor_chunk_);
  for (size_t i = 0u; i < num_centers_; ++i) {
    Chunk* to_insert = app::center_table->newChunk(numToId(i));
    center_chunks_.push_back(CHECK_NOTNULL(to_insert));
  }
  for (size_t i = 0u; i < degree_ * degree_; ++i) {
    Chunk* to_insert = app::association_table->newChunk(numToId(i));
    membership_chunks_.push_back(CHECK_NOTNULL(to_insert));
  }
  *data_chunk = descriptor_chunk_;
  *center_chunks = center_chunks_;
  *membership_chunks = membership_chunks_;
  KmeansSubdivisionView exporter(degree_, max_dimension_, num_centers_,
                                 center_chunks_, membership_chunks_,
                                 descriptor_chunk_);
  exporter.insert(descriptors, *centers, membership);

  // launch gnuplot
  if (FLAGS_enable_visualization) {
    if (FLAGS_gnuplot_persist) {
      gnuplot_ = popen("gnuplot --persist", "w");
    } else {
      gnuplot_ = popen("gnuplot", "w");
    }
    fprintf(gnuplot_, "set xrange [0:%f]\n", area_width);
    fprintf(gnuplot_, "set yrange [0:%f]\n", area_width);
    fputs("set size square\nset key off\n", gnuplot_);

    plot(descriptors, *centers, membership);
  }
}

void KmeansSubdivisionHoarder::refresh() {
  if (FLAGS_enable_visualization) {
    DescriptorVector descriptors, centers;
    std::vector<unsigned int> membership;
    KmeansSubdivisionView view(degree_, max_dimension_, num_centers_,
                               center_chunks_, membership_chunks_,
                               descriptor_chunk_);
    view.fetch(&descriptors, &centers, &membership);
    plot(descriptors, centers, membership);
  }
}

void KmeansSubdivisionHoarder::refreshThread() {
  while (!terminate_refresh_thread_) {
    refresh();
    sleep(1);
  }
}

void KmeansSubdivisionHoarder::startRefreshThread() {
  if (FLAGS_enable_visualization) {
    terminate_refresh_thread_ = false;
    refresh_thread_ =
        std::thread(&KmeansSubdivisionHoarder::refreshThread, this);
  }
}

void KmeansSubdivisionHoarder::stopRefreshThread() {
  if (FLAGS_enable_visualization) {
    terminate_refresh_thread_ = true;
    refresh_thread_.join();
  }
}

void KmeansSubdivisionHoarder::plot(
    const DescriptorVector& descriptors, const DescriptorVector& centers,
    const std::vector<unsigned int>& membership) {
  fputs("plot ", gnuplot_);
  for (size_t i = 0; i < centers.size(); ++i) {
    fputs("'-' w p, ", gnuplot_);
  }
  fputs("'-' w p lt rgb \"black\" pt 7 ps 2\n", gnuplot_);
  for (size_t i = 0; i < centers.size(); ++i) {
    for (size_t j = 0; j < descriptors.size(); ++j) {
      if (membership[j] == i) {
        fprintf(gnuplot_, "%f %f\n", descriptors[j][0], descriptors[j][1]);
      }
    }
    fputs("-1 -1\n", gnuplot_);  // so there are no empty plot groups
    fputs("e\n", gnuplot_);
  }
  for (const DescriptorType center : centers) {
    fprintf(gnuplot_, "%f %f\n", center[0], center[1]);
  }
  fputs("e\n", gnuplot_);
  fflush(gnuplot_);
}

} /* namespace benchmarks */
} /* namespace map_api */
