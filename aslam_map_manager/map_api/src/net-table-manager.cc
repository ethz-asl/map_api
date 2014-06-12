#include "map-api/net-table-manager.h"

namespace map_api {

void NetTableManager::addTable(std::unique_ptr<TableDescriptor>* descriptor) {
  std::pair<std::unordered_map<std::string, NetCRTable>::iterator, bool>
  inserted = tables_.insert(
      std::make_pair((*descriptor)->name(), NetCRTable()));
  CHECK(inserted.second);
  CHECK(inserted.first->second.init(descriptor));
}

NetCRTable& NetTableManager::getTable(const std::string& name) {
  std::unordered_map<std::string, NetCRTable>::iterator found =
      tables_.find(name);
  CHECK(found != tables_.end());
  return *found;
}

} /* namespace map_api */
