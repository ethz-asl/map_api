#ifndef MAP_API_NET_TABLE_MANAGER_H_
#define MAP_API_NET_TABLE_MANAGER_H_

#include <memory>

#include "map-api/net-cr-table.h"
#include "map-api/table-descriptor.h"

namespace map_api {

class NetTableManager {
 public:
  void addTable(std::unique_ptr<TableDescriptor>* descriptor);
  NetCRTable& getTable(const std::string& name);
  void clear();
 private:
  NetTableManager() = default;
  NetTableManager(const NetTableManager&) = delete;
  NetTableManager& operator =(const NetTableManager&) = delete;
  ~NetTableManager() = default;
  friend class MapApiCore;

  std::unordered_map<std::string, std::unique_ptr<NetCRTable> > tables_;
};

} /* namespace map_api */

#endif /* MAP_API_NET_TABLE_MANAGER_H_ */
