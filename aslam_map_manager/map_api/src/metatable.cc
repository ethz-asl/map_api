#include <map-api/metatable.h>

namespace map_api {

REVISION_PROTOBUF(proto::TableDescriptor);

static const std::string kDescriptorField;

const std::string Metatable::kNameField = "name";
const std::string Metatable::kDescriptorField = "descriptor";

const std::string Metatable::name() const {
  return "metatable";
}

void Metatable::defineFieldsCRDerived() {
  addField<std::string>(kNameField);
  addField<proto::TableDescriptor>(kDescriptorField);
}

MEYERS_SINGLETON_INSTANCE_FUNCTION_IMPLEMENTATION(Metatable);

} /* namespace map_api */
