#include "map-api/net-cr-table.h"

#include "map-api/chunk-manager.h"

namespace map_api {

const std::string NetCRTable::kChunkIdField = "chunk_id";

NetCRTable::~NetCRTable() {}

bool NetCRTable::init() {
  active_chunks_.clear();
  insert_chunk_.reset();
  return CRTable::init();
}

void NetCRTable::defineFieldsCRDerived() {
  addField<Id>(kChunkIdField);
  defineFieldsNetCRDerived();
}

bool NetCRTable::netInsert(const std::weak_ptr<Chunk>& chunk, Revision* query) {
  // TODO(tcies) ensureDefaultFields
  // TODO(tcies) set chunk id fields -> add ID property to Chunk
  // insertion into local table
  if (!CRTable::rawInsertImpl(query)) {
    return false;
  }
  std::shared_ptr<Chunk> locked_chunk = chunk.lock();
  if (!locked_chunk) {
    //TODO(tcies) rollback? fatal? same below
    return false;
  }
  return locked_chunk->insert(*query);
}

int NetCRTable::netFindFast(
    const std::string& key, const Revision& valueHolder, const Time& time,
    std::unordered_map<Id, std::shared_ptr<Revision> >* dest) {
  int local_result =
      CRTable::rawFindByRevisionImpl(key, valueHolder, time, dest);
  if (local_result) {
    return local_result;
  }
  return ChunkManager::instance().findAmongPeers(
      *this, key, valueHolder, time, dest);
}


} // namespace map_api
