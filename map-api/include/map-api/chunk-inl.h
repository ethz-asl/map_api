#ifndef MAP_API_CHUNK_INL_H_
#define MAP_API_CHUNK_INL_H_

#include "map-api/table-data-container-base.h"

namespace map_api {

template <typename RequestType>
void Chunk::fillMetadata(RequestType* destination) {
  CHECK_NOTNULL(destination);
  destination->mutable_metadata()->set_table(
      this->table_data_container_->name());
  id().serialize(destination->mutable_metadata()->mutable_chunk_id());
}

inline common::Id Chunk::id() const { return id_; }

inline void Chunk::syncLatestCommitTime(const Revision& item) {
  LogicalTime commit_time = item.getModificationTime();
  if (commit_time > latest_commit_time_) {
    latest_commit_time_ = commit_time;
  }
}

inline LogicalTime Chunk::getLatestCommitTime() {
  distributedReadLock();
  LogicalTime result = latest_commit_time_;
  distributedUnlock();
  return result;
}

}  // namespace map_api

#endif  // MAP_API_CHUNK_INL_H_
