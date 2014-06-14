#include "map-api/net-cr-table.h"

#include <glog/logging.h>

#include "map-api/chunk-manager.h"

namespace map_api {

const std::string NetCRTable::kChunkIdField = "chunk_id";

bool NetCRTable::init(std::unique_ptr<TableDescriptor>* descriptor) {
  (*descriptor)->addField<Id>(kChunkIdField);
  cache_.reset(new CRTableRAMCache);
  chunk_manager_.reset(new ChunkManager);
  CHECK(cache_->init(descriptor));
  CHECK(chunk_manager_->init(cache_.get()));
  return true;
}

std::shared_ptr<Revision> NetCRTable::getTemplate() const {
  return cache_->getTemplate();
}

std::weak_ptr<Chunk> NetCRTable::newChunk() const {
  return chunk_manager_->newChunk();
}

bool NetCRTable::insert(const std::weak_ptr<Chunk>& chunk, Revision* query) {
  CHECK_NOTNULL(query);
  std::shared_ptr<Chunk> locked_chunk = chunk.lock();
  CHECK(locked_chunk);
  query->set(kChunkIdField, locked_chunk->id());
  CHECK(cache_->insert(query));
  CHECK(locked_chunk->insert(*query)); // TODO(tcies) insert into cache in here
  return true;
}

void NetCRTable::dumpCache(
      const Time& time,
      std::unordered_map<Id, std::shared_ptr<Revision> >* destination) {
  CHECK_NOTNULL(destination);
  cache_->dump(time, destination);
}

std::weak_ptr<Chunk> NetCRTable::connectTo(const Id& chunk_id,
                                           const PeerId& peer) {
  // TODO(tcies) ever slight code smell
  return chunk_manager_->connectTo(chunk_id, peer);
}

} // namespace map_api
