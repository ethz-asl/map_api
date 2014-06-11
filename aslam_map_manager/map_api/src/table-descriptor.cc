#include "table-descriptor.h"

#include <glog/logging.h>

namespace map_api {

TableDescriptor::~TableDescriptor() {}

void TableDescriptor::addField(const std::string& name,
                       proto::TableFieldDescriptor_Type type){
  // make sure the field has not been defined yet
  for (int i = 0; i < fields_size(); ++i){
    if (fields(i).name().compare(name) == 0){
      LOG(FATAL) << "In descriptor of " << name() << ": Field " << name <<
          " defined twice!" << std::endl;
    }
  }
  // otherwise, proceed with adding field
  proto::TableFieldDescriptor *field = add_fields();
  field->set_name(name);
  field->set_type(type);
}

} /* namespace map_api */
