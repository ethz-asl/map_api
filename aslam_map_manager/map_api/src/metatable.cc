#include <map-api/metatable.h>

namespace map_api {

REVISION_PROTOBUF(proto::TableDescriptor);

Metatable::Metatable(const Id& owner) : CRTableInterface(owner) {}

Metatable::~Metatable() {}

bool Metatable::init() {
  return setup("metatable");
}

bool Metatable::define() {
  addField<std::string>("name");
  addField<proto::TableDescriptor>("descriptor");
  return true;
}

} /* namespace map_api */
