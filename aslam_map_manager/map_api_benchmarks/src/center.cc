#include "map_api_benchmarks/center.h"

#include <glog/logging.h>

namespace map_api {
namespace benchmarks {

const map_api::benchmarks::CenterId& Center::id() const {
  return id_;
}

void Center::setId(const map_api::benchmarks::CenterId& id) {
  id_ = id;
  set_id(id.hexString());
}

bool Center::parse(const std::string& str) {
  return ParseFromString(str)
      && id_.fromHexString(proto::Center::id());
}

Eigen::VectorXd Center::getData() const {
  const Eigen::Map<const Eigen::VectorXd> eigen_data(
      data().data(), data_size(), 1);
  Eigen::VectorXd ret_val = eigen_data;
  return ret_val;
}

void Center::setData(const Eigen::VectorXd& data_in) {
  while (data_size() < data_in.rows()) {
    add_data(0);
  }

  Eigen::Map<Eigen::VectorXd> eigen_data(mutable_data()->mutable_data(),
                                         data_size(), 1);
  eigen_data = data_in;
}

}  // namespace benchmarks
}  // namespace map_api
