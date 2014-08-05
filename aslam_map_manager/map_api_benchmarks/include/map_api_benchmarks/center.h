#ifndef MAP_API_BENCHMARKS_CENTER_H_
#define MAP_API_BENCHMARKS_CENTER_H_

#include <unordered_set>

#include <Eigen/Core>

#include <map_api_benchmarks/common.h>

#include "kmeans.pb.h"

namespace map_api {
namespace benchmarks {

class Center : public proto::Center {
 public:
  virtual ~Center() {}

  virtual const map_api::benchmarks::CenterId& id() const;
  void setId(const map_api::benchmarks::CenterId& id);

  virtual Eigen::VectorXd getData() const;
  void setData(const Eigen::VectorXd& data);

  bool parse(const std::string& str);

  EIGEN_MAKE_ALIGNED_OPERATOR_NEW

 private:
  using proto::Center::set_id;
  using proto::Center::mutable_id;
  using proto::Center::set_data;
  using proto::Center::mutable_data;
  using proto::Center::ParseFromString;
  map_api::benchmarks::CenterId id_;
};
}  // namespace benchmarks
}  // namespace map_api

#endif  // MAP_API_BENCHMARKS_CENTER_H_
