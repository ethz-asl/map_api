#include <utility>
#include "map-api/chunk-manager.h"

namespace map_api {
void ChunkManagerBase::requestParticipationAllChunks() {
  for (const std::pair<Id, Chunk*>& item : active_chunks_) {
    CHECK_NOTNULL(item->second);
    item->second->requestParticipation();
  }
}

Chunk* ChunkManagerChunkSize::getChunkForItem(const Revision& revision) {
  int item_size = revision.ByteSize();
  int total_size = current_chunk_size_ + item_size;

  if (total_size > max_chunk_size_ || current_chunk_ == nullptr) {
    current_chunk_ = underlying_table_->newChunk();
    active_chunks_.insert(std::make_pair(current_chunk_->id(), current_chunk_));
    current_chunk_size_ = 0;
  }
  current_chunk_size_ += item_size;
  return current_chunk_;
}

}  // namespace map_api
