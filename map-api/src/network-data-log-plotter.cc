// Copyright (C) 2014-2017 Titus Cieslewski, ASL, ETH Zurich, Switzerland
// You can contact the author at <titus at ifi dot uzh dot ch>
// Copyright (C) 2014-2015 Simon Lynen, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014-2015, Marcin Dymczyk, ASL, ETH Zurich, Switzerland
// Copyright (c) 2014, Stéphane Magnenat, ASL, ETH Zurich, Switzerland
//
// This file is part of Map API.
//
// Map API is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Map API is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Map API. If not, see <http://www.gnu.org/licenses/>.

#include <limits>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <map-api-common/gnuplot-interface.h>

#include "map-api/internal/network-data-log.h"

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  CHECK_EQ(argc, 2) << "Usage: " << argv[0] << " <network log file>";

  const std::string file_name(argv[1]);

  typedef map_api::internal::NetworkDataLog Log;
  Log::TypeCumSums type_cum_sums;
  Log::getCumSums(file_name, &type_cum_sums);

  // Set start time to lowest event time.
  double start_time = std::numeric_limits<double>::max();
  for (const Log::TypeCumSums::value_type& cum_sum : type_cum_sums) {
    const double time_of_first_event = cum_sum.second(0, 0);
    start_time = std::min(start_time, time_of_first_event);
  }
  for (Log::TypeCumSums::value_type& cum_sum : type_cum_sums) {
    cum_sum.second.row(0) -= Eigen::RowVectorXd::Constant(
        1, cum_sum.second.cols(), start_time);
  }

  map_api_common::GnuplotInterface plot(file_name);
  plot.setLegendPosition("left top");
  plot.setXLabel("time [s]");
  plot.setYLabel("cumulutive transmitted data [bytes]");
  plot.plotSteps(type_cum_sums);

  return 0;
}
