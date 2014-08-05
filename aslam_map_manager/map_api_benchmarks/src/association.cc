#include "map_api_benchmarks/association.h"

#include <glog/logging.h>

namespace map_api {
namespace benchmarks {

const map_api::benchmarks::AssociationId& Association::id() const {
  return id_;
}

void Association::setId(const map_api::benchmarks::AssociationId& id) {
  id_ = id;
  set_id(id.hexString());
}

const map_api::benchmarks::DataPointId& Association::dataPointId() const {
  return data_point_id_;
}

void Association::setDataPointId(
    const map_api::benchmarks::DataPointId& data_point_id) {
  data_point_id_ = data_point_id;
  set_data_point_id(data_point_id.hexString());
}

const map_api::benchmarks::CenterId& Association::centerId() const {
  return center_id_;
}

void Association::setCenterId(const map_api::benchmarks::CenterId& center_id) {
  center_id_ = center_id;
  set_center_id(center_id.hexString());
}

bool Association::parse(const std::string& str) {
  return ParseFromString(str)
      && data_point_id_.fromHexString(proto::DataPoint::id())
      && center_id_.fromHexString(proto::Center::id());
}

}  // namespace benchmarks
}  // namespace map_api
