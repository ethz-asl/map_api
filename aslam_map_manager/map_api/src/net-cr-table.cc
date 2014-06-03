#include "map-api/net-cr-table.h"

#include "map-api/chunk-manager.h"

namespace map_api {

const std::string NetCRTable::kChunkIdField = "chunk_id";

bool NetCRTable::init() {
  active_chunks_.clear();
  insert_chunk_.reset();
  return CRTable::init();
}

void NetCRTable::defineFieldsCRDerived() {
  addField<Id>(kChunkIdField);
  defineFieldsNetCRDerived();
}

bool NetCRTable::rawInsertImpl(Revision* query) const {
  // insertion into local table
  if (!CRTable::rawInsertImpl(query)) {
    return false;
  }
  // connect to a chunk to insert the query to
  std::shared_ptr<Chunk> insert_chunk = insert_chunk_.lock();
  if (!insert_chunk) {
    // TODO(tcies) can't because const, fix this (temporary: assign in init())
    //insert_chunk_ = ChunkManager::instance().newChunk(*this);
    //insert_chunk = insert_chunk_.lock();
    CHECK(insert_chunk) << "Couldn't get a new chunk from the manager";
  }

  CHECK(insert_chunk->insert(*query)) << "Couldn't share newly inserted item";
  return true;
}

int NetCRTable::rawFindByRevisionImpl(
    const std::string& key, const Revision& valueHolder, const Time& time,
    std::unordered_map<Id, std::shared_ptr<Revision> >* dest)  const {
  // FastFind-flavor: look locally first
  int local_result = CRTable::rawFindByRevisionImpl(key, valueHolder, time,
                                                    dest);
  if (local_result) {
    return local_result;
  }
  return ChunkManager::instance().findAmongPeers(
      *this, key, valueHolder, time, dest);
}


} // namespace map_api
