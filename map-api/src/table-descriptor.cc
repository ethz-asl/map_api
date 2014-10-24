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

} /* namespace map_api */
