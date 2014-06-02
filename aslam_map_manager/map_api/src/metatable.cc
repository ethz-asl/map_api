#include <map-api/metatable.h>

namespace map_api {

REVISION_PROTOBUF(proto::TableDescriptor);

static const std::string kDescriptorField;

const std::string Metatable::kNameField = "name";
const std::string Metatable::kDescriptorField = "descriptor";

Metatable::~Metatable() {}

const std::string Metatable::name() const {
  return "metatable";
}

void Metatable::define() {
  addField<std::string>(kNameField);
  addField<proto::TableDescriptor>(kDescriptorField);
}

Metatable& Metatable::instance() {
  return CRTable::meyersInstance<Metatable>();
}

} /* namespace map_api */
