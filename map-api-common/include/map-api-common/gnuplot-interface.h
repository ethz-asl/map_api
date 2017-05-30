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
// along with Map API.  If not, see <http://www.gnu.org/licenses/>.

#ifndef MAP_API_COMMON_GNUPLOT_INTERFACE_H_
#define MAP_API_COMMON_GNUPLOT_INTERFACE_H_

#include <initializer_list>
#include <string>
#include <unordered_map>
#include <vector>

#include <Eigen/Dense>
#include <gflags/gflags.h>

DECLARE_bool(use_gnuplot);

namespace map_api_common {

class GnuplotInterface {
 public:
  GnuplotInterface(bool persist, const std::string& title);
  explicit GnuplotInterface(const std::string& title);
  GnuplotInterface();

  ~GnuplotInterface();
  
  void operator<<(const std::string& string);

  template <int Rows>
  void operator<<(const Eigen::Matrix<double, Rows, Eigen::Dynamic>& data) {
    if (FLAGS_use_gnuplot) {
      for (int i = 0; i < data.cols(); ++i) {
        for (int d = 0; d < Rows; ++d) {
          fprintf(pipe_, "%lf ", data(d, i));
        }
        operator<<("");  // Adds newline.
      }
      operator<<("e");
    }
  }

  template <int Rows>
  void operator<<(const Eigen::Matrix<int, Rows, Eigen::Dynamic>& data) {
    if (FLAGS_use_gnuplot) {
      for (int i = 0; i < data.cols(); ++i) {
        for (int d = 0; d < Rows; ++d) {
          fprintf(pipe_, "%d ", data(d, i));
        }
        operator<<("");  // Adds newline.
      }
      operator<<("e");
    }
  }

  template <typename Type>
  void plotSteps(const std::unordered_map<
      std::string, Eigen::Matrix<Type, 2, Eigen::Dynamic>>& legend_data) {
    CHECK(!legend_data.empty());
    typedef std::unordered_map<
        std::string, Eigen::Matrix<Type, 2, Eigen::Dynamic>> LegendDataMap;
    fputs("plot ", pipe_);
    bool first = true;
    for (const typename LegendDataMap::value_type& data : legend_data) {
      if (first) {
        first = false;
      } else {
        fputs(", ", pipe_);
      }
      fprintf(pipe_, "'-' w steps title '%s' lw 4", data.first.c_str());
    }
    fputs("\n", pipe_);
    for (const typename LegendDataMap::value_type& data : legend_data) {
      operator<<(data.second);
    }
  }

  void setXLabel(const std::string& label);

  void setYLabel(const std::string& label);
  void setYLabels(const std::string& label_1, const std::string& label_2);

  void setTitle(const std::string& title);
  void setTitle();  // Uses member "title_".

  // {left | right | center} {top | bottom | center}
  void setLegendPosition(const std::string& position);

 private:
  FILE* pipe_;
  // Because a title replot is sometimes required.
  const std::string title_;
};

}  // namespace map_api_common

#endif  // MAP_API_COMMON_GNUPLOT_INTERFACE_H_
