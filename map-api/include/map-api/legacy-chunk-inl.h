#ifndef MAP_API_LEGACY_CHUNK_INL_H_
#define MAP_API_LEGACY_CHUNK_INL_H_

#include "map-api/chunk-data-container-base.h"

namespace map_api {

template <typename RequestType>
void LegacyChunk::fillMetadata(RequestType* destination) const {
  CHECK_NOTNULL(destination);
  destination->mutable_metadata()->set_table(this->data_container_->name());
  id().serialize(destination->mutable_metadata()->mutable_chunk_id());
}

inline void LegacyChunk::syncLatestCommitTime(const Revision& item) {
  LogicalTime commit_time = item.getModificationTime();
  if (commit_time > latest_commit_time_) {
    latest_commit_time_ = commit_time;
  }
}

}  // namespace map_api

#endif  // MAP_API_LEGACY_CHUNK_INL_H_
