#include <map-api/table-descriptor.h>
#include <glog/logging.h>

namespace map_api {

TableDescriptor::~TableDescriptor() {}

void TableDescriptor::addField(int index, proto::Type type) {
  CHECK_EQ(fields_size(), index) << "Fields must be added in-order";
  add_fields(type);
}

void TableDescriptor::setName(const std::string& name) {
  set_name(name);
}

void TableDescriptor::setSpatialIndex(
    const SpatialIndex::BoundingBox& extent,
    const std::vector<uint32_t>& subdivision) {
  CHECK_EQ(subdivision.size(), extent.size());
  extent.serialize(mutable_spatial_extent());
  clear_spatial_subdivision();
  for (uint32_t dimension_division : subdivision) {
    add_spatial_subdivision(dimension_division);
  }
}

} /* namespace map_api */
