#ifndef MAP_API_CHUNK_INL_H_
#define MAP_API_CHUNK_INL_H_

namespace map_api {

template <typename RequestType>
void Chunk::fillMetadata(RequestType* destination) {
  CHECK_NOTNULL(destination);
  destination->mutable_metadata()->set_table(this->underlying_table_->name());
  destination->mutable_metadata()->set_chunk_id(id().hexString());
}

inline Id Chunk::id() const { return id_; }

inline void Chunk::syncLatestCommitTime(const Revision& item) {
  LogicalTime commit_time;
  if (underlying_table_->type() == CRTable::Type::CR) {
    item.get(CRTable::kInsertTimeField, &commit_time);
  } else {
    CHECK(underlying_table_->type() == CRTable::Type::CRU);
    item.get(CRUTable::kUpdateTimeField, &commit_time);
  }
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
