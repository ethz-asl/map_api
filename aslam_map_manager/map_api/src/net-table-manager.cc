#include "map-api/net-table-manager.h"

namespace map_api {

void NetTableManager::addTable(std::unique_ptr<TableDescriptor>* descriptor) {
  std::pair<std::unordered_map<std::string, std::unique_ptr<NetCRTable> >::
  iterator, bool> inserted = tables_.insert(
      std::make_pair((*descriptor)->name(), std::unique_ptr<NetCRTable>()));
  CHECK(inserted.second);
  inserted.first->second.reset(new NetCRTable);
  CHECK(inserted.first->second->init(descriptor));
}

NetCRTable& NetTableManager::getTable(const std::string& name) {
  std::unordered_map<std::string, std::unique_ptr<NetCRTable> >::iterator
  found = tables_.find(name);
  CHECK(found != tables_.end());
  return *found->second;
}

void NetTableManager::clear() {
  tables_.clear();
}

} /* namespace map_api */
