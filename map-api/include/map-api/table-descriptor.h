#ifndef MAP_API_TABLE_DESCRIPTOR_H_
#define MAP_API_TABLE_DESCRIPTOR_H_

#include <string>
#include <vector>

#include "map-api/spatial-index.h"
#include "./core.pb.h"

namespace map_api {
class Revision;

class TableDescriptor : private proto::TableDescriptor {
  friend class ChunkDataContainerBase;
  friend class NetTableManager;
  friend class Revision;

 public:
  virtual ~TableDescriptor();

  using proto::TableDescriptor::name;

  template <typename Type>
  void addField(int index);
  void addField(int index, proto::Type type);

  void setName(const std::string& name);

  void setSpatialIndex(const SpatialIndex::BoundingBox& extent,
                       const std::vector<size_t>& subdivision);

  std::shared_ptr<Revision> getTemplate() const;
};

}  // namespace map_api

#include "./table-descriptor-inl.h"

#endif  // MAP_API_TABLE_DESCRIPTOR_H_
