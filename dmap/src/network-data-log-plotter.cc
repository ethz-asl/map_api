#include <limits>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <multiagent-mapping-common/gnuplot-interface.h>

#include "dmap/internal/network-data-log.h"

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  CHECK_EQ(argc, 2) << "Usage: " << argv[0] << " <network log file>";

  const std::string file_name(argv[1]);

  typedef dmap::internal::NetworkDataLog Log;
  Log::TypeCumSums type_cum_sums;
  Log::getCumSums(file_name, &type_cum_sums);

  // Set start time to lowest event time.
  double start_time = std::numeric_limits<double>::max();
  for (const Log::TypeCumSums::value_type& cum_sum : type_cum_sums) {
    start_time = std::min(start_time, cum_sum.second(0, 0));
  }
  for (Log::TypeCumSums::value_type& cum_sum : type_cum_sums) {
    cum_sum.second.row(0) -= Eigen::RowVectorXd::Constant(
        1, cum_sum.second.cols(), start_time);
  }

  common::GnuplotInterface plot(file_name);
  plot.setLegendPosition("left top");
  plot.setXLabel("time [s]");
  plot.setYLabel("cumulutive transmitted data [bytes]");
  plot.plotSteps(type_cum_sums);

  return 0;
}
