#include "map-api/table-descriptor.h"

#include <glog/logging.h>

namespace map_api {

TableDescriptor::~TableDescriptor() {
  // TODO Auto-generated destructor stub
}

void TableDescriptor::addField(const std::string& field_name,
                       proto::TableFieldDescriptor_Type type){
  // make sure the field has not been defined yet
  for (int i = 0; i < fields_size(); ++i){
    if (fields(i).name().compare(field_name) == 0){
      LOG(FATAL) << "In descriptor of " << name() << ": Field " << field_name <<
          " defined twice!" << std::endl;
    }
  }
  // otherwise, proceed with adding field
  proto::TableFieldDescriptor *field = add_fields();
  field->set_name(field_name);
  field->set_type(type);
}

void TableDescriptor::setName(const std::string& name) {
  set_name(name);
}

} /* namespace map_api */
