#ifndef MAP_API_BENCHMARKS_DATA_POINT_H_
#define MAP_API_BENCHMARKS_DATA_POINT_H_

#include <unordered_set>

#include <Eigen/Core>

#include <map_api_benchmarks/common.h>

#include "kmeans.pb.h"

namespace map_api {
namespace benchmarks {

class DataPoint : public proto::DataPoint {
 public:
  ~DataPoint() {}

  virtual const map_api::benchmarks::DataPointId& id() const;
  void setId(const map_api::benchmarks::DataPointId& id);

  virtual Eigen::VectorXd getData() const;
  void setData(const Eigen::VectorXd& data);

  bool parse(const std::string& str);

  EIGEN_MAKE_ALIGNED_OPERATOR_NEW

 private:
  using proto::DataPoint::set_id;
  using proto::DataPoint::mutable_id;
  using proto::DataPoint::set_data;
  using proto::DataPoint::mutable_data;
  using proto::DataPoint::ParseFromString;
  map_api::benchmarks::DataPointId id_;
};
}  // namespace benchmarks
}  // namespace map_api

#endif  // MAP_API_BENCHMARKS_DATA_POINT_H_
