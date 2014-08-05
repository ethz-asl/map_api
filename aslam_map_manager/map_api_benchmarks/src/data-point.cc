#include "map_api_benchmarks/data-point.h"

#include <glog/logging.h>

namespace map_api {
namespace benchmarks {

const map_api::benchmarks::DataPointId& DataPoint::id() const {
  return id_;
}

void DataPoint::setId(const map_api::benchmarks::DataPointId& id) {
  id_ = id;
  set_id(id.hexString());
}

bool DataPoint::parse(const std::string& str) {
  return ParseFromString(str)
      && id_.fromHexString(proto::DataPoint::id());
}

Eigen::VectorXd DataPoint::getData() const {
  const Eigen::Map<const Eigen::VectorXd> eigen_data(
      data().data(), data_size(), 1);
  Eigen::VectorXd ret_val = eigen_data;
  return ret_val;
}

void DataPoint::setData(const Eigen::VectorXd& data_in) {
  while (data_size() < data_in.rows()) {
    add_data(0);
  }

  Eigen::Map<Eigen::VectorXd> eigen_data(mutable_data()->mutable_data(),
                                         data_size(), 1);
  eigen_data = data_in;
}

}  // namespace benchmarks
}  // namespace map_api
