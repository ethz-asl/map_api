#ifndef MAP_API_TABLE_DESCRIPTOR_H_
#define MAP_API_TABLE_DESCRIPTOR_H_

#include <string>
#include <vector>

#include "map-api/spatial-index.h"
#include "./core.pb.h"

namespace map_api {

class TableDescriptor : private proto::TableDescriptor {
  friend class CRTable;
  friend class NetTableManager;
  friend class Revision;

 public:
  virtual ~TableDescriptor();

  template <typename Type>
  void addField(int index);
  void addField(int index, proto::Type type);

  void setName(const std::string& name);

  void setSpatialIndex(const SpatialIndex::BoundingBox& extent,
                       const std::vector<size_t>& subdivision);
};

}  // namespace map_api

#include "./table-descriptor-inl.h"

#endif  // MAP_API_TABLE_DESCRIPTOR_H_
