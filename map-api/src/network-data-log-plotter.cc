#include <gflags/gflags.h>
#include <glog/logging.h>
#include <multiagent-mapping-common/gnuplot-interface.h>

#include "map-api/internal/network-data-log.h"

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  CHECK_EQ(argc, 2) << "Usage: " << argv[0] << " <network log file>";

  const std::string file_name(argv[1]);

  map_api::internal::NetworkDataLog::TypeCumSums type_cum_sums;
  map_api::internal::NetworkDataLog::getCumSums(file_name, &type_cum_sums);

  common::GnuplotInterface plot(file_name);
  plot.setLegendPosition("left top");
  plot.setXLabel("time since epoch [s]");
  plot.setYLabel("cumulutive transmitted data [bytes]");
  plot.plotSteps(type_cum_sums);

  return 0;
}
