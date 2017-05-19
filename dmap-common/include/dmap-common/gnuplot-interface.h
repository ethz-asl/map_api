#ifndef DMAP_COMMON_GNUPLOT_INTERFACE_H_
#define DMAP_COMMON_GNUPLOT_INTERFACE_H_

#include <initializer_list>
#include <string>
#include <unordered_map>
#include <vector>

#include <Eigen/Dense>
#include <gflags/gflags.h>

DECLARE_bool(use_gnuplot);

namespace dmap_common {

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

}  // namespace dmap_common

#endif  // DMAP_COMMON_GNUPLOT_INTERFACE_H_
