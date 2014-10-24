#ifndef MAP_API_TABLE_DESCRIPTOR_H_
#define MAP_API_TABLE_DESCRIPTOR_H_

#include <string>

#include "./core.pb.h"
#include <map-api/revision.h>

namespace map_api {

class TableDescriptor : private proto::TableDescriptor {
 public:
  virtual ~TableDescriptor();

  template <typename Type>
  void addField(int index);
  void addField(int index, proto::Type type);
  void setName(const std::string& name);

  using proto::TableDescriptor::has_name;
  using proto::TableDescriptor::name;
  using proto::TableDescriptor::fields_size;
  using proto::TableDescriptor::fields;

  using proto::TableDescriptor::DebugString;
  using proto::TableDescriptor::ParseFromString;
  using proto::TableDescriptor::SerializeAsString;
};

}  // namespace map_api

#include "./table-descriptor-inl.h"

#endif  // MAP_API_TABLE_DESCRIPTOR_H_
