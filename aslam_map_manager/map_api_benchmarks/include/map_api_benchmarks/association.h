#ifndef MAP_API_BENCHMARKS_ASSOCIATION_H_
#define MAP_API_BENCHMARKS_ASSOCIATION_H_

#include <unordered_set>

#include <Eigen/Core>

#include <map_api_benchmarks/common.h>

#include "kmeans.pb.h"

namespace map_api {
namespace benchmarks {

class Association : public proto::Association {
 public:
  ~Association() {}

  virtual const map_api::benchmarks::DataPointId& id() const;
  void setId(const map_api::benchmarks::DataPointId& id);

  virtual const map_api::benchmarks::CenterId& centerId() const;
  void setCenterId(const map_api::benchmarks::CenterId& id);

  bool parse(const std::string& str);

  EIGEN_MAKE_ALIGNED_OPERATOR_NEW

 private:
  using proto::Association::set_id;
  using proto::Association::mutable_id;
  using proto::Association::set_center_id;
  using proto::Association::mutable_center_id;
  using proto::Association::ParseFromString;
  map_api::benchmarks::DataPointId id_;
  map_api::benchmarks::CenterId center_id_;
};
}  // namespace benchmarks
}  // namespace map_api

#endif  // MAP_API_BENCHMARKS_ASSOCIATION_H_
