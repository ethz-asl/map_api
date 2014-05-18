#include <map-api/metatable.h>

namespace map_api {

REVISION_PROTOBUF(proto::TableDescriptor);

Metatable::~Metatable() {}

const std::string Metatable::name() const {
  return "metatable";
}

void Metatable::define() {
  addField<std::string>("name");
  addField<proto::TableDescriptor>("descriptor");
}

} /* namespace map_api */
