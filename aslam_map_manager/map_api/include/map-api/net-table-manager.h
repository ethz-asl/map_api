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
 private:
  std::unordered_map<std::string, NetCRTable> tables_;
};

} /* namespace map_api */

#endif /* MAP_API_NET_TABLE_MANAGER_H_ */
