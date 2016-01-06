#include "map-api/table-descriptor.h"

#include <glog/logging.h>

#include "map-api/revision.h"

namespace map_api {

TableDescriptor::~TableDescriptor() {}

void TableDescriptor::addField(int index, proto::Type type) {
  CHECK_EQ(fields_size(), index) << "Fields must be added in-order";
  add_fields(type);
}

void TableDescriptor::setName(const std::string& name) {
  set_name(name);
}

void TableDescriptor::setSpatialIndex(const SpatialIndex::BoundingBox& extent,
                                      const std::vector<size_t>& subdivision) {
  CHECK_EQ(subdivision.size(), extent.size());
  extent.serialize(mutable_spatial_extent());
  clear_spatial_subdivision();
  for (size_t dimension_division : subdivision) {
    add_spatial_subdivision(dimension_division);
  }
}

std::shared_ptr<Revision> TableDescriptor::getTemplate() const {
  std::shared_ptr<Revision> result;
  Revision::fromProto(std::unique_ptr<proto::Revision>(new proto::Revision),
                      &result);
  // add editable fields
  for (int i = 0; i < fields_size(); ++i) {
    result->addField(i, fields(i));
  }
  return result;
}

} /* namespace map_api */
