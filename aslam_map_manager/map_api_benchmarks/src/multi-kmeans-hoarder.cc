#include "map_api_benchmarks/multi-kmeans-hoarder.h"

#include <glog/logging.h>

namespace map_api {
namespace benchmarks {

DEFINE_bool(gnuplot_persist, false, "if set, gnuplot detaches from test");

void MultiKmeansHoarder::init(
    const DescriptorVector& descriptors, const DescriptorVector& centers,
    const std::vector<unsigned int>& membership, const Scalar area_width,
    map_api::Id* data_chunk_id, map_api::Id* center_chunk_id,
    map_api::Id* membership_chunk_id) {
  CHECK_NOTNULL(data_chunk_id);
  CHECK_NOTNULL(center_chunk_id);
  CHECK_NOTNULL(membership_chunk_id);

  // TODO(tcies) database business

  if (FLAGS_gnuplot_persist) {
    gnuplot_ = popen("gnuplot --persist", "w");
  } else {
    gnuplot_ = popen("gnuplot", "w");
  }
  fprintf(gnuplot_, "set xrange [0:%f]\n", area_width);
  fprintf(gnuplot_, "set yrange [0:%f]\n", area_width);
  fputs("set size square\nset key off\n", gnuplot_);
  fputs("plot '-' w p\n", gnuplot_);
  for (const DescriptorType descriptor : descriptors) {
    fprintf(gnuplot_, "%f %f\n", descriptor[0], descriptor[1]);
  }
  fputs("e\n", gnuplot_);
  fflush(gnuplot_);
}

void MultiKmeansHoarder::refresh() {
  // TODO(tcies) implement
  CHECK(false);
}

} /* namespace benchmarks */
} /* namespace map_api */
