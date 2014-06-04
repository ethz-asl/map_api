#include "map-api/chunk-manager.h"

#include "map-api/cr-table.h"

namespace map_api {

MEYERS_SINGLETON_INSTANCE_FUNCTION_IMPLEMENTATION(ChunkManager);

int ChunkManager::findAmongPeers(
    const CRTable& table, const std::string& key, const Revision& valueHolder,
    const Time& time,
    std::unordered_map<Id, std::shared_ptr<Revision> >* dest) {
  // TODO(tcies) implement
  return 0;
}


} // namespace map_api
